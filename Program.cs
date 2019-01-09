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
                Description = "Copies input azure table to a stream, optionally remaining open and polling for changes as they occur"
            };

            app.HelpOption(inherited: true);

            var sourceOption    = app.Option("-s|--source <URI>", "Source Azure Table", CommandOptionType.SingleValue).IsRequired();
            var sourceKeyOption = app.Option("-k|--sourceKey <KEY>", "Azure storage key", CommandOptionType.SingleValue).IsRequired();
            var filterOption    = app.Option("--filter <STRING>", "PK or RK filter", CommandOptionType.SingleValue);
            var pkOption        = app.Option("--partition <STRING>", "PK partitioning rule", CommandOptionType.SingleValue);
            var parallelOption  = app.Option("--parallel <NUM>", "Parallelism", CommandOptionType.SingleValue);

            var destOption = app.Option("-d|--dest <PATH>", "Destiation file path", CommandOptionType.SingleValue).IsRequired();

            // Execute command line interface
            app.OnExecute(() => ExecuteAsync().GetAwaiter().GetResult());

            return app.Execute(args);

            async Task ExecuteAsync() 
            {
                var stopwatch = new Stopwatch();
                stopwatch.Start();

                var parallelInt = int.TryParse(parallelOption.Value(), out var _p) ? _p : default(int?);

                using (var file = new FileConsumer(destOption.Value(), parallelInt))
                {
                    var ts = new TableStream(sourceOption.Value(), sourceKeyOption.Value(), file)
                    {
                        Filter    = filterOption.Value(),
                        Partition = pkOption.Value()
                    };
                    
                    if (parallelInt.HasValue)
                        ts.Parallelism = parallelInt.Value;

                    await ts.ProcessAsync();
                    await file.DoneAsync();
                }

                stopwatch.Stop();
                Console.WriteLine($"Elapsed: {stopwatch.Elapsed}");
            }
        }
        
    }
}
