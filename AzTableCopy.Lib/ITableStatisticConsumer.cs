using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;

namespace Wivuu.AzTableCopy
{
    public interface ITableStatisticConsumer : IDisposable
    {
        Task TakeAsync(int numEntries);

        Task DoneAsync();
    }
}