using RocksDb.Extensions.Helpers;

public static partial class RocksDbExtensions
{
    public static string GetString(this RocksDbSharp.RocksDb db, int key) => db.GetString(BitConverter.GetBytes(key));
    public static byte[] GetBytes(this RocksDbSharp.RocksDb db, int key) => db.GetBytes(BitConverter.GetBytes(key));

    public static string GetString(this RocksDbSharp.RocksDb db, long key) => db.GetString(BitConverter.GetBytes(key));
    public static byte[] GetBytes(this RocksDbSharp.RocksDb db, long key) => db.GetBytes(BitConverter.GetBytes(key));

    public static string Get(this RocksDbSharp.RocksDb db, Int128 key) => db.GetString(NumericConverter.GetBytes(key));
    public static byte[] GetBytes(this RocksDbSharp.RocksDb db, Int128 key) => db.GetBytes(NumericConverter.GetBytes(key));

    public static string Get(this RocksDbSharp.RocksDb db, Guid key) => db.GetString(key.ToByteArray());
    public static byte[] GetBytes(this RocksDbSharp.RocksDb db, Guid key) => db.GetBytes(key.ToByteArray());

}
