using Net.RocksDb.Domain;
using NUlid;
using RocksDbSharp;

namespace Net.RocksDb.Tests;

public static class PerfomanceTests
{
    public static IReadOnlyCollection<Document> PrepareDocuments(List<string> documentIds, int count = 10_000_000)
    {
        print($"Start PrepareDocuments");

        DateTime date = DateTime.Parse("2000.01.01");
        int dateIncrement = 0;

        var documents = Enumerable.Range(0, count).AsParallel()
            .Select(i => new Document
            {
                DocumentId = Ulid.NewUlid().ToString(),
                Date = date.AddSeconds(dateIncrement++),
                Number = $"DOC-1020304050{dateIncrement}",
                DocumentType = 1,
                OperationType = 1,
                DocumentStatus = 1,
            })
            .ToArray();

        for (int i = 0; i < count; i += 1000)
        {
            var document = documents[i];
            documentIds.Add(document.DocumentId);
        }

        /*
        foreach (var document in documents)
        {
            documentIds.Add(document.DocumentId);
        }
        */
        print($"End PrepareDocuments");

        return documents;
    }

    public static void RunWrite(RocksDbSharp.RocksDb db, IReadOnlyCollection<Document> documents, List<string> documentIds)
    {
        print("RunWrite");

        foreach (var document in documents)
        {
            db.Put(document.DocumentId, document.Json());
        }

        print($"End RunWrite");
    }

    public static void RunWriteParallel(RocksDbSharp.RocksDb db, IReadOnlyCollection<Document> documents, List<string> documentIds)
    {
        print($"Start Put");

        documents.AsParallel().ForAll(document =>
        {
            db.Put(document.DocumentId, document.Json());
        });

        print($"End Put");
    }

    public static void RunWriteBatch(RocksDbSharp.RocksDb db, IReadOnlyCollection<Document> documents, List<string> documentIds)
    {
        print($"Prepare Batch");
        WriteBatch writeBatch = new WriteBatch();
        foreach (var document in documents)
        {
            writeBatch.Put(document.DocumentId, document.Json());
        }
        print($"End Prepare Batch");

        print($"Start Put");
        db.Write(writeBatch);
        print($"End Put");

        print($"Flush");
        db.Flush(new FlushOptions());
        print($"End Flush");
    }


    public static void RunRead(RocksDbSharp.RocksDb db, IReadOnlyCollection<string> documentIds)
    {
        print("Start RunRead");

        foreach (var documentId in documentIds)
        {
            print($"Start Read");
            var documentJson = db.Get(documentId);
            print($"End Read");
            var document = Document.From(documentJson);
            print(document);
        };
    }

    public static void RunReadParallelRepeat(RocksDbSharp.RocksDb db, IReadOnlyCollection<string> documentIds)
    {
        print("Start RunRead");

        Enumerable.Repeat(documentIds, 100_000)
            .SelectMany(x => x)
            .AsParallel()
            .ForAll(documentId =>
            {
                print($"Start Read");
                var documentJson = db.Get(documentId);
                print($"End Read");
                var document = Document.From(documentJson);
                print(document);
            });
    }

    public static void RunReadParallelRepeatReadOnly(RocksDbSharp.RocksDb db, 
        IReadOnlyCollection<string> documentIds, int repeat = 100_000)
    {
        print("Start RunReadParallelRepeatReadOnly");

        Enumerable.Repeat(documentIds, repeat)
            .SelectMany(x => x)
            .AsParallel()
            .ForAll(documentId =>
            {
                var documentJson = db.Get(documentId);
            });

        print("End RunReadParallelRepeatReadOnly");
    }
}
