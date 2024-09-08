using RocksDbSharp;

namespace Net.RocksDb.Tests;

public static class ParallelTests
{
    public static async Task Run()
    {
        print("Start");

        var options = new DbOptions()
            .SetCreateIfMissing(true);

        string directory = Path.Combine(Directory.GetCurrentDirectory(), "Data");
        if (Directory.Exists(directory) == false) Directory.CreateDirectory(directory);

        print($"Selected Path: {directory}");

        print($"try open:");
        using (var db = RocksDbSharp.RocksDb.Open(options, directory))
        {
            print($"Success!!");

            await Parallel.ForEachAsync(Enumerable.Range(0, 1000), CancellationToken.None, async (x, cancellationToken) =>
            {
                print($"Put. CurrentThread: {Thread.CurrentThread.ManagedThreadId}");
                db.Put("key", "value");
                print($"Success!!");
                await Task.Delay(1);

                print($"Get. CurrentThread: {Thread.CurrentThread.ManagedThreadId}");
                string value = db.Get("key");
                print($"Success!!");
                await Task.Delay(1);
            });


            Console.ReadKey();
        }
    }
}
