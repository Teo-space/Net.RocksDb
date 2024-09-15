using RocksDbSharp;

namespace DotnetRocksDb.Tests.TestsSimple;

public static class TaskWhenAllTests
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

            List<Task> tasks = new List<Task>();

            for (int i = 0; i < 1000; i++)
            {
                var task1 = async () =>
                {
                    print($"Put. CurrentThread: {Thread.CurrentThread.ManagedThreadId}");
                    db.Put("key", "value");
                    print($"Success!!");
                    await Task.Delay(1);
                };

                var task2 = async () =>
                {
                    print($"Get. CurrentThread: {Thread.CurrentThread.ManagedThreadId}");
                    string value = db.Get("key");
                    print($"Success!!");
                    await Task.Delay(1);
                };

                tasks.Add(task1());
                tasks.Add(task2());
            }
            await Task.WhenAll(tasks);

            Console.ReadKey();
        }

    }
}
