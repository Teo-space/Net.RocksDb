namespace RocksDb.Extensions;

public static partial class RocksDbExtensions
{
    public static IEnumerable<KeyValuePair<string, string>> ScanAscending(this RocksDbSharp.RocksDb database, string keyPrefix)
    {
        using (var iterator = database.NewIterator())
        {
            iterator.SeekToFirst();
            iterator.Seek(keyPrefix);

            while (iterator.Valid())
            {
                string key = iterator.StringKey();
                string value = iterator.StringValue();
                if (key.StartsWith(keyPrefix))
                {
                    yield return new KeyValuePair<string, string>(key, value);
                }
                else
                {
                    break;
                }
                iterator.Next();
            }
        }
    }

    public static IEnumerable<KeyValuePair<string, string>> ScanDescending(this RocksDbSharp.RocksDb database, string keyPrefix)
    {
        using (var iterator = database.NewIterator())
        {
            iterator.SeekToLast();
            iterator.SeekForPrev(keyPrefix);

            while (iterator.Valid())
            {
                string key = iterator.StringKey();
                string value = iterator.StringValue();
                if (key.StartsWith(keyPrefix))
                {
                    yield return new KeyValuePair<string, string>(key, value);
                }
                else
                {
                    break;
                }
                iterator.Prev();
            }
        }
    }
}
