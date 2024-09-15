using System.Text;
using System.Text.Json;

namespace DotnetRocksDb.Tests.Domain;

public record MessageKey(long ServerId, long ChannelId, long MessageId)
{
    public static implicit operator byte[](MessageKey messageKey)
    {
        byte[] key = new byte[][]
        {
            BitConverter.GetBytes(messageKey.ServerId),
            BitConverter.GetBytes(messageKey.ChannelId),
            BitConverter.GetBytes(messageKey.MessageId)
        }
        .SelectMany(x => x).ToArray();

        return key;
    }
}

public record Message
{
    public required MessageKey Key { get; init; }

    public required DateTime Date { get; init; }

    public required string Text { get; set; }


    public string Json() => JsonSerializer.Serialize(this);
    public byte[] Bytes() => Encoding.UTF8.GetBytes(JsonSerializer.Serialize(this));

    public static Document? From(byte[] data) => JsonSerializer.Deserialize<Document>(data);
    public static Document? From(string json) => JsonSerializer.Deserialize<Document>(json);
}