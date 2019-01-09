using System;
using System.Diagnostics;
using System.Threading.Tasks;
using McMaster.Extensions.CommandLineUtils;

namespace Wivuu.AzTableCopy
{
    class Program
    {
        static int Main(string[] args)
        {
            var app = new CommandLineApplication
            {
                Name        = "AzTableCopy",
                Description = "Copies input azure table to files, or use as a library"
            };

            app.HelpOption(inherited: true);

            var sourceOption    = app.Option("-s|--source <URI>", "Source Azure Table", CommandOptionType.SingleValue).IsRequired();
            var sourceKeyOption = app.Option("-k|--sourceKey <KEY>", "Azure storage key", CommandOptionType.SingleValue).IsRequired();
            var filterOption    = app.Option("--filter <STRING>", "PK or RK filter", CommandOptionType.SingleValue);
            var pkOption        = app.Option("--partition <STRING>", "PK partitioning rule", CommandOptionType.SingleValue);
            var parallelOption  = app.Option("--parallel <NUM>", "Parallelism", CommandOptionType.SingleValue);

            var destOption      = app.Option("-d|--dest <PATH>", "Destination file path", CommandOptionType.SingleValue);
            var httpOption      = app.Option("-w|--web", "Web server -- alternative to destination", CommandOptionType.NoValue);
            var httpPortOption  = app.Option("--port", "Web port (default 8081)", CommandOptionType.SingleValue);

            // Execute command line interface
            app.OnExecute(() => ExecuteAsync().GetAwaiter().GetResult());

            return app.Execute(args);

            async Task ExecuteAsync() 
            {
                var stopwatch = new Stopwatch();
                stopwatch.Start();

                var parallelInt = int.TryParse(parallelOption.Value(), out var _p) ? _p : default(int?);

                ITableEntryConsumer consumer;

                if (destOption.HasValue())
                    consumer = new FileConsumer(destOption.Value(), parallelInt);
                else if (httpOption.HasValue())
                {
                    var httpPort = int.TryParse(httpPortOption.Value(), out var _h) ? _h : default(int?);
                    consumer = new WebConsumer(httpPort);
                }
                else {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("Must choose destination (path) or web option");
                    Console.ResetColor();
                    return;
                }

                using (consumer)
                {
                    var ts = new TableStream(sourceOption.Value(), sourceKeyOption.Value(), consumer)
                    {
                        Filter    = filterOption.Value(),
                        Partition = pkOption.Value()
                    };
                    
                    if (parallelInt.HasValue)
                        ts.Parallelism = parallelInt.Value;

                    await ts.ProcessAsync();
                    await consumer.DoneAsync();
                }

                stopwatch.Stop();
                Console.WriteLine($"Elapsed: {stopwatch.Elapsed}");
            }
        }
        
    }
}
