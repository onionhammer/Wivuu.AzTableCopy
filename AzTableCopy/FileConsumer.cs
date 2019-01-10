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
    public class FileConsumer : GenericConsumer
    {
        public string Destination { get; }

        public FileConsumer(string path)
        {
            var dirName = path;

            if (!Directory.Exists(dirName))
                Directory.CreateDirectory(dirName);

            Destination = path;

            Console.WriteLine($"Writing data to {Destination}...");
            
            StartConsumers(N: 4);
        }

        public override async Task ConsumeAsync(int index)
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
                        row.Properties,
                        converter
                    );

                    csv.WriteField(data);

                    await csv.NextRecordAsync();
                }
            }
        }
    }
}