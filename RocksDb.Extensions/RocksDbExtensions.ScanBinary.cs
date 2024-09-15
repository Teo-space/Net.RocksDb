namespace RocksDb.Extensions;

public static partial class RocksDbExtensions
{
    public static IEnumerable<KeyValuePair<byte[], byte[]>> ScanAscending(this RocksDbSharp.RocksDb database, byte[] keyPrefix)
    {
        using (var iterator = database.NewIterator())
        {
            iterator.SeekToFirst();
            iterator.Seek(keyPrefix);

            while (iterator.Valid())
            {
                byte[] key = iterator.Key();
                byte[] value = iterator.Value();
                if (key.Length > 0 && key.Length >= keyPrefix.Length)
                {
                    if (keyPrefix.SequenceEqual(key.Take(keyPrefix.Length)))
                    {
                        yield return new KeyValuePair<byte[], byte[]>(key, value);
                    }
                    else break;
                }
                else break;

                iterator.Next();
            }
        }
    }

    public static IEnumerable<KeyValuePair<byte[], byte[]>> ScanDescending(this RocksDbSharp.RocksDb database, byte[] keyPrefix)
    {
        using (var iterator = database.NewIterator())
        {
            iterator.SeekToLast();
            iterator.SeekForPrev(keyPrefix);

            while (iterator.Valid())
            {
                byte[] key = iterator.Key();
                byte[] value = iterator.Value();
                if (key.Length > 0 && key.Length >= keyPrefix.Length)
                {
                    if (keyPrefix.SequenceEqual(key.Take(keyPrefix.Length)))
                    {
                        yield return new KeyValuePair<byte[], byte[]>(key, value);
                    }
                    else break;
                }
                else break;

                iterator.Prev();
            }
        }
    }

    public static void ScanRangeAscending(this RocksDbSharp.RocksDb database, byte[] keyMin, byte[] keyMax)
    {

    }

    public static void ScanRangeDescending(this RocksDbSharp.RocksDb database, byte[] keyMin, byte[] keyMax)
    {

    }
}
