using System.Text.Json;

namespace DotnetRocksDb.Tests.Domain;

public sealed record Document
{
    public required string DocumentId { get; set; }
    public required string Number { get; set; }
    public required DateTime Date { get; set; }

    public required int DocumentType { get; set; }
    public required int OperationType { get; set; }
    public required int DocumentStatus { get; set; }
    public required int Index { get; set; }

    public string Json() => JsonSerializer.Serialize(this);

    public static Document? From(byte[] data) => JsonSerializer.Deserialize<Document>(data);
    public static Document? From(string json) => JsonSerializer.Deserialize<Document>(json);

}
