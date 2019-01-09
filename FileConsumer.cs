using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CsvHelper;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;

namespace Wivuu.AzTableCopy
{
    public class FileConsumer : ITableEntryConsumer, IDisposable
    {
        public TaskCompletionSource<int> Completion { get; }
        public BlockingCollection<DynamicTableEntity> PubSub { get; }
        public string Destination { get; }
        public int? Parallel { get; }

        public FileConsumer(string path, int? parallel)
        {
            var dirName = path;

            if (!Directory.Exists(dirName))
                Directory.CreateDirectory(dirName);

            Completion  = new TaskCompletionSource<int>();
            PubSub      = new BlockingCollection<DynamicTableEntity>(100_000);
            Destination = path;
            Parallel    = parallel;

            StartConsumers(Environment.ProcessorCount);
        }

        private void StartConsumers(int processorCount)
        {
            var N = Parallel ?? Environment.ProcessorCount;

            Console.WriteLine($"Writing data to {Destination}...");

            _ = Task.WhenAll(
                // Create N consumers                
                from i in Enumerable.Range(0, N)
                select Task.Run(() => Consumer(i))
            ).ContinueWith(_ => Completion.SetResult(0));

            async Task Consumer(int index)
            {
                var filename   = $"table-{index}.csv";
                var outputPath = Path.Join(Destination, filename);
                var converter  = new TableEntityConverter();

                using (var tw  = File.CreateText(outputPath))
                using (var csv = new CsvWriter(tw))
                {
                    csv.WriteField("PartitionKey");
                    csv.WriteField("RowKey");
                    csv.WriteField("Timestamp");
                    csv.WriteField("Data");
                    await csv.NextRecordAsync();

                    foreach (var row in PubSub.GetConsumingEnumerable())
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
            }
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
            PubSub.Dispose();
        }
    }
}