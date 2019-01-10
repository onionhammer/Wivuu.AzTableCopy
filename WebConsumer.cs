using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using CsvHelper;
using Microsoft.AspNetCore;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;

namespace Wivuu.AzTableCopy
{
    public class WebConsumer : ITableEntryConsumer, IStartup
    {
        public int HttpPort { get; }
        public BlockingCollection<DynamicTableEntity> PubSub { get; }
        public TaskCompletionSource<int> Completion { get; }
        public IWebHost Host { get; }

        public WebConsumer(int? httpPort)
        {
            HttpPort   = httpPort ?? 8081;
            PubSub     = new BlockingCollection<DynamicTableEntity>(100_000);
            Completion = new TaskCompletionSource<int>();

            // Start web server
            Host = WebHost
                .CreateDefaultBuilder()
                .ConfigureServices(services =>
                {
                    services.AddSingleton<IStartup>(this);
                })
                .UseKestrel(options => {
                    options.Listen(IPAddress.Any, HttpPort);
                })
                // .UseStartup<WebConsumerStartup>()
                .Build();

            _ = Host.RunAsync();
        }

        public IServiceProvider ConfigureServices(IServiceCollection services)
        {
            return services.BuildServiceProvider();
        }

        public void Configure(IApplicationBuilder app)
        {
            app.Run(async context =>
            {
                var converter = new TableEntityConverter();

                using (var ms = new MemoryStream(8192))
                {
                    do
                    {
                        using (var tw  = new StreamWriter(ms, Encoding.UTF8, 4096, true))
                        using (var csv = new CsvWriter(tw))
                        {
                            foreach (var row in PubSub.Take(1000))
                            {
                                csv.WriteField(row.PartitionKey);
                                csv.WriteField(row.RowKey);
                                csv.WriteField(row.Timestamp.UtcDateTime.ToString("O"));
                                
                                var data = JsonConvert.SerializeObject(
                                    row.Properties.Select(p => (
                                        key:   p.Key,
                                        value: p.Value.PropertyAsObject
                                    )),
                                    converter
                                );

                                csv.WriteField(data);

                                await csv.NextRecordAsync();
                            }
                        }

                        await context.Response.Body.WriteAsync(ms.ToArray());

                        ms.Position = 0;
                        ms.SetLength(0);
                    }
                    while (PubSub.IsAddingCompleted == false);
                }

                await context.Response.Body.FlushAsync();

                Completion.SetResult(0);
            });
        }

        public Task TakeAsync(IList<DynamicTableEntity> entries)
        {
            for (var i = 0; i < entries.Count; ++i)
                PubSub.Add(entries[i]);

            return Task.CompletedTask;
        }

        public async Task DoneAsync()
        {
            PubSub.CompleteAdding();

            await Completion.Task;
            
            await Host.StopAsync();
        }
        
        public void Dispose()
        {
            // Dispose webserver
            Host.Dispose();

            PubSub.Dispose();
        }
    }
}