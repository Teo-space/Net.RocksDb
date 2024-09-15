using System.Text.Json;

public static partial class RocksDbExtensions
{
    public static T? Get<T>(this RocksDbSharp.RocksDb db, string key) where T : class
    {
        string value = db.Get(key);
        if (string.IsNullOrEmpty(value)) return default(T);

        return JsonSerializer.Deserialize<T>(value);
    }

    public static T? Get<T>(this RocksDbSharp.RocksDb db, byte[] key) where T : class
    {
        byte[] value = db.Get(key);
        if (value == null || value.Length == 0) return default(T);

        return JsonSerializer.Deserialize<T>(value);
    }

    public static void Put<T>(this RocksDbSharp.RocksDb db, string key, T value) where T : class
    {
        string json = JsonSerializer.Serialize(value);

        db.Put(key, json);
    }

    public static void Put<T>(this RocksDbSharp.RocksDb db, byte[] key, T value) where T : class
    {
        string json = JsonSerializer.Serialize(value);

        db.Put(key, json);
    }

}
