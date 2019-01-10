using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;
using Wivuu.AzTableCopy;

namespace Wivuu.AzTableCopy
{
    public abstract class GenericConsumer : ITableEntryConsumer
    {
        bool disposed = false;
        
        protected TaskCompletionSource<int> Completion { get; }
            = new TaskCompletionSource<int>();

        protected BlockingCollection<DynamicTableEntity> PubSub { get; }
            = new BlockingCollection<DynamicTableEntity>();

        protected void StartConsumers(int N)
        {
            _ = Task.WhenAll(
                // Create N consumers                
                from i in Enumerable.Range(0, N)
                select Task.Run(() => ConsumeAsync(i))
            ).ContinueWith(_ => Completion.SetResult(0));
        }

        public abstract Task ConsumeAsync(int index);

        public Task TakeAsync(IList<DynamicTableEntity> entries)
        {
            for (var i = 0; i < entries.Count; ++i)
                PubSub.Add(entries[i]);

            return Task.CompletedTask;
        }

        public virtual async Task DoneAsync()
        {
            PubSub.CompleteAdding();

            await Completion.Task;
        }

        public void Dispose()
        {
            Dispose(true);

            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposed)
                return;

            if (disposing)
                PubSub.Dispose();

            disposed = true;
        }

        ~GenericConsumer()
        {
            Dispose(false);
        }
    }
}