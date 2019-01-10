# AzTableCopy

Copies input azure table to files

## CLI Usage

```
dotnet tool install Wivuu.AzTableCopy -g
```

```
AzTableCopy [options]

Options:
  -?|-h|--help          Show help information
  -s|--source <URI>     Source Azure Table
  -k|--sourceKey <KEY>  Azure storage key
  --filter <STRING>     PK or RK filter
  --partition <STRING>  PK partitioning rule
  --parallel <NUM>      Parallelism
  -d|--dest <PATH>      Destination file path
```

## Library Usage

Add to your project

```
dotnet add package Wivuu.AzTableCopy.Lib
```

Implement a custom consumer (in this example `MyCustomConsumer`), and pass it to the table stream.

```C#
using (var consumer = new MyCustomConsumer())
{
    // Create a new TableStream, indicating where to access the data from
    var ts = new TableStream(storageUri, storageKey, consumer)
    {
        Filter = tableQueryFilter
    };

    // Starts sending data to consumer
    await ts.ProcessAsync();

    // Marks the process as complete, will wait for consumer
    // to finish processing all data
    await consumer.DoneAsync();
}
```

## Next:
- Built in web server consumer for streaming data over HTTP (beta)
