using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;

namespace Wivuu.AzTableCopy
{
    internal class WebConsumer : ITableEntryConsumer
    {
        public int HttpPort { get; }
        public BlockingCollection<DynamicTableEntity> PubSub { get; }
        public TaskCompletionSource<int> Completion { get; }

        public WebConsumer(int? httpPort)
        {
            HttpPort   = httpPort ?? 8081;
            PubSub     = new BlockingCollection<DynamicTableEntity>(100_000);
            Completion = new TaskCompletionSource<int>();

            // TODO: Start web server & consumer

            Console.WriteLine($"Serving table csv on http://localhost:{HttpPort}");
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
        }
        
        public void Dispose()
        {
            // TODO: Close webserver

            PubSub.Dispose();
        }
    }
}