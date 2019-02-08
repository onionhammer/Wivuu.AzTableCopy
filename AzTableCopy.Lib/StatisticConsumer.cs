using System;
using System.Threading;
using System.Threading.Tasks;

namespace Wivuu.AzTableCopy
{
    public class StatisticConsumer : ITableStatisticConsumer
    {
        public int Total = 0;

        public Task TakeAsync(int numEntries)
        {
            Interlocked.Add(ref Total, numEntries);
            return Task.CompletedTask;
        }

        public Task DoneAsync()
        {
            Console.WriteLine($"Total entities: {Total.ToString("#,0")}");
            return Task.CompletedTask;
        }

        public void Dispose()
        {
        }
    }
}