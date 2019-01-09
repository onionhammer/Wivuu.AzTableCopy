using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Table;

namespace Wivuu.AzTableCopy
{
    public class TableStream
    {
        public CloudTable Table { get; }
        public string Filter { get; set; }
        public string Partition { get; set; }
        public int Parallelism { get; set; } = Environment.ProcessorCount * 4;
        public ITableEntryConsumer Consumer { get; }

        public TableStream(string source, string sourceKey, ITableEntryConsumer consumer)
        {
            if (!Uri.TryCreate(source, UriKind.Absolute, out var fullUri))
                throw new Exception($"Invalid URI: ${source}");

            var account = Regex.Match(fullUri.Host, @"^[^\.]+").Groups[0].Value;

            var tableUri = new UriBuilder
            {
                Scheme = fullUri.Scheme,
                Host   = fullUri.Host,
            };

            // Create table client from URI and Key
            var tableClient = new CloudTableClient(
                tableUri.Uri,
                new StorageCredentials(account, sourceKey));

            Table    = tableClient.GetTableReference(fullUri.Segments[1]);
            Consumer = consumer;
        }

        public async Task ProcessAsync()
        {
            // Ensure table exists
            if (!await Table.ExistsAsync())
                throw new Exception($"Table {Table.Name} does not exist");

            var partitionBy = Partition;
            var filterBy    = Filter;

            var queries = from partition in GetPartitions(partitionBy)
                          let filter = IncludeFilter(partition)
                          select new TableQuery
                          {
                              FilterString = filter
                          };

            foreach (var group in GroupByN(Parallelism, queries))
            {
                await Task.WhenAll(
                    group.Select(GenerateDataForQuery)
                );
            }
        }

        async Task GenerateDataForQuery(TableQuery query)
        {
            TableContinuationToken next = default;

            try
            {
                do
                {
                    var segment = await Table.ExecuteQuerySegmentedAsync(query, next);

                    if (segment.Results.Count > 0)
                        await Consumer.TakeAsync(segment.Results);
                    
                    next = segment.ContinuationToken;
                }
                while (next != null);
            }
            catch (StorageException error)
            {
                Console.WriteLine(error.RequestInformation.Exception.ToString());
            }
        }

        IEnumerable<IEnumerable<T>> GroupByN<T>(int N, IEnumerable<T> items) =>
            items
                .Select((value, index) => (value, index / N))
                .GroupBy(p => p.Item2)
                .Select(t => t.Select(r => r.value));
        
        IEnumerable<string> GetPartitions(string partitionBy)
        {
            if (!string.IsNullOrEmpty(partitionBy))
            {
                // TableQuery.GenerateFilterCondition
                var partitions = partitionBy.Split('#');

                yield return TableQuery.GenerateFilterCondition("PartitionKey", "lt", partitions[0]);

                foreach (var (a, b) in partitions.Zip(partitions.Skip(1), (a, b) => (a, b)))
                {
                    yield return TableQuery.CombineFilters(
                        TableQuery.GenerateFilterCondition("PartitionKey", "ge", a),
                        "and",
                        TableQuery.GenerateFilterCondition("PartitionKey", "lt", b)
                    );
                }

                yield return TableQuery.GenerateFilterCondition("PartitionKey", "ge", partitions.Last());
            }
            else
                yield return string.Empty;
        }

        string IncludeFilter(string pkFilter)
        {
            if (!string.IsNullOrEmpty(pkFilter) && !string.IsNullOrWhiteSpace(Filter))
                return TableQuery.CombineFilters(pkFilter, "and", Filter);
            else if (!string.IsNullOrWhiteSpace(pkFilter))
                return pkFilter;
            else
                return Filter;
        }
    }
}