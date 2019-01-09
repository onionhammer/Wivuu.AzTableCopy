using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;

namespace Wivuu.AzTableCopy
{
    internal class WebConsumer : ITableEntryConsumer
    {
        private int? httpPort;

        public WebConsumer(int? httpPort)
        {
            this.httpPort = httpPort;
        }

        public Task DoneAsync()
        {
            throw new System.NotImplementedException();
        }

        public Task TakeAsync(IList<DynamicTableEntity> entries)
        {
            throw new System.NotImplementedException();
        }
        
        public void Dispose()
        {
            throw new System.NotImplementedException();
        }
    }
}