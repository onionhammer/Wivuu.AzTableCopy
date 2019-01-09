using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;

namespace Wivuu.AzTableCopy
{
    public interface ITableEntryConsumer : IDisposable
    {
        Task TakeAsync(IList<DynamicTableEntity> entries);

        Task DoneAsync();
    }
}