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
            //Int128 int128 => BitConverter.GetBytes(int128),//dotnet 9
            Int128 int128 => GetBytes(int128),
            ushort sh => BitConverter.GetBytes(sh),
            uint i => BitConverter.GetBytes(i),
            ulong l => BitConverter.GetBytes(l),

            _ => throw new NotSupportedException()
        };
    }

    public static byte[] GetBytes(Int128 value)
    {
        byte[] bytes = new byte[16];

        bytes[0] = (byte)(value >> 8 * 16);
        bytes[1] = (byte)(value >> 8 * 15);
        bytes[2] = (byte)(value >> 8 * 14);
        bytes[3] = (byte)(value >> 8 * 13);
        bytes[4] = (byte)(value >> 8 * 12);
        bytes[5] = (byte)(value >> 8 * 11);
        bytes[6] = (byte)(value >> 8 * 10);
        bytes[7] = (byte)(value >> 8 * 9);
        bytes[8] = (byte)(value >> 8 * 8);
        bytes[9] = (byte)(value >> 8 * 7);
        bytes[10] = (byte)(value >> 8 * 6);
        bytes[11] = (byte)(value >> 8 * 5);
        bytes[12] = (byte)(value >> 8 * 4);
        bytes[13] = (byte)(value >> 8 * 3);
        bytes[14] = (byte)(value >> 8 * 2);
        bytes[15] = (byte)(value >> 8 * 1);
        bytes[16] = (byte)value;

        return bytes;
    }
}