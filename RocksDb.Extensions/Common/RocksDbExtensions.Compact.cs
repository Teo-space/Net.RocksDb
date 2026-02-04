using RocksDbSharp;

namespace RocksDb.Extensions;

public static partial class RocksDbExtensions
{
    public static void CompactDatabaseBinary(this RocksDbSharp.RocksDb database)
    {
        byte[] fromKey = new byte[0];
        byte[] toKey = new byte[0];

        print($"Get Range Keys");
        using (var iterator = database.NewIterator())
        {
            iterator.SeekToFirst();
            fromKey = iterator.Key();

            iterator.SeekToLast();
            toKey = iterator.Key();
        }
        print($"Done");

        print($"Start CompactRange");
        database.CompactRange(fromKey, toKey);
        print($"End CompactRange");

        print($"Start Flush");
        database.Flush(new FlushOptions());
        print($"End Flush");
    }

    public static void CompactDatabase(this RocksDbSharp.RocksDb database)
    {
        string fromKey = string.Empty;
        string toKey = string.Empty;

        print($"Get Range Keys");
        using (var iterator = database.NewIterator())
        {
            iterator.SeekToFirst();
            fromKey = iterator.StringKey();

            iterator.SeekToLast();
            toKey = iterator.StringKey();
        }
        print($"Done");

        print($"Start CompactRange");
        database.CompactRange(fromKey, toKey);
        print($"End CompactRange");

        print($"Start Flush");
        database.Flush(new FlushOptions());
        print($"End Flush");
    }

}
