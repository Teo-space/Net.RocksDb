using System.Text;

public static partial class RocksDbExtensions
{
    public static string GetString(this RocksDbSharp.RocksDb db, string key) => db.Get(key);
    public static byte[] GetBytes(this RocksDbSharp.RocksDb db, string key) => db.Get(Encoding.UTF8.GetBytes(key));
    public static string GetString(this RocksDbSharp.RocksDb db, byte[] key) => Encoding.UTF8.GetString(db.Get(key));
    public static byte[] GetBytes(this RocksDbSharp.RocksDb db, byte[] key) => db.Get(key);

    public static void Put(this RocksDbSharp.RocksDb db, string key, string value) => db.Put(key, value);
    public static void Put(this RocksDbSharp.RocksDb db, string key, byte[] value) => db.Put(Encoding.UTF8.GetBytes(key), value);
    public static void Put(this RocksDbSharp.RocksDb db, byte[] key, string value) => db.Put(key, Encoding.UTF8.GetBytes(value));
    public static void Put(this RocksDbSharp.RocksDb db, byte[] key, byte[] value) => db.Put(key, value);

    //public static void Delete(this RocksDb db, string key) => db.Delete(key);
    //public static void Delete(this RocksDb db, byte[] key) => db.Delete(key);

}
