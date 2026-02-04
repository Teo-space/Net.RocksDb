using System.Numerics;

namespace RocksDb.Extensions.Helpers;

public static class NumericConverter
{
    public static byte[] ToBytes<T>(params T[] values) where T : IBinaryInteger<T>
    {
        return values.SelectMany(x => ToBytes(x)).ToArray();
    }

    public static byte[] ToBytes<T>(T value) where T : IBinaryInteger<T>
    {
        return value switch
        {
            byte b => new byte[1] { b },
            char c => BitConverter.GetBytes(c),
            short sh => BitConverter.GetBytes(sh),
            int i => BitConverter.GetBytes(i),
            long l => BitConverter.GetBytes(l),
            Int128 int128 => BitConverter.GetBytes(int128),
            ushort sh => BitConverter.GetBytes(sh),
            uint i => BitConverter.GetBytes(i),
            ulong l => BitConverter.GetBytes(l),

            _ => throw new NotSupportedException()
        };
    }
}