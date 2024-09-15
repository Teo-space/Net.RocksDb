using RocksDbSharp;

namespace DotnetRocksDb.Tests.TestsSimple;

public static class SimpleTest
{
    public static void Run()
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

            print($"Put");
            db.Put("key", "value");
            print($"Success!!");

            print($"Get");
            string value = db.Get("key");
            print($"Success!!");

            print($"Remove");
            db.Remove("key");
            print($"Success!!");

            Console.ReadKey();
        }

    }
}
