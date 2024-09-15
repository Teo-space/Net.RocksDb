using DotnetRocksDb.Tests.Domain;
using RocksDbSharp;

namespace DotnetRocksDb.Tests.TestsWithStringKey;

public static class PerfomanceTests
{
    public static IReadOnlyCollection<Document> PrepareDocuments(List<string> documentIds, int count = 100_000)
    {
        print($"Start PrepareDocuments");

        DateTime date = DateTime.Parse("2000.01.01");
        int dateIncrement = 0;

        var documents = new List<Document>();
        for (int i = 1; i <= count; i++)
        {
            for (int j = 1; j <= 100; j++)
            {
                dateIncrement++;

                var document = new Document
                {
                    DocumentId = $"{i}.{j}",
                    Date = date.AddSeconds(dateIncrement),
                    Number = $"DOC-1020304050{dateIncrement}",
                    DocumentType = 1,
                    OperationType = 1,
                    DocumentStatus = 1,
                    Index = dateIncrement
                };

                documents.Add(document);

                if (i >= count / 2 && i <= (count / 2) + (count / 10))
                {
                    documentIds.Add(document.DocumentId);
                }
            }
        }

        print($"End PrepareDocuments");

        return documents;
    }
    //60 sec
    public static void RunWrite(RocksDbSharp.RocksDb db, IReadOnlyCollection<Document> documents)
    {
        print("RunWrite");

        foreach (var document in documents)
        {
            db.Put(document.DocumentId, document.Json());
        }

        print($"End RunWrite");

        print($"Flush");
        db.Flush(new FlushOptions());
        print($"End Flush");
    }
    //50 sec - but 100% cpu - not efficiency
    public static void RunWriteParallel(RocksDbSharp.RocksDb db, IReadOnlyCollection<Document> documents)
    {
        print($"Start Put");

        documents.AsParallel().ForAll(document =>
        {
            db.Put(document.DocumentId, document.Json());
        });

        print($"End Put");

        print($"Flush");
        db.Flush(new FlushOptions());
        print($"End Flush");
    }
    //4 sec insert, 4 sec CompactDatabase
    public static void RunWriteBatch(RocksDbSharp.RocksDb db, IReadOnlyCollection<Document> documents)
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
        IReadOnlyCollection<string> documentIds, int repeat = 1)
    {
        print($@"Start RunReadParallelRepeatReadOnly for {documentIds.Count} lines, repeat: {repeat}. 
TotalCount: {documentIds.Count * repeat}");

        Enumerable.Repeat(documentIds, repeat)
            .SelectMany(x => x)
            .AsParallel()
            .ForAll(documentId =>
            {
                var documentJson = db.Get(documentId);
            });

        print("End RunReadParallelRepeatReadOnly");
    }


    public static void FindAll(RocksDbSharp.RocksDb db, string keyPrefix)
    {
        using (var iterator = db.NewIterator(/*readOptions: new ReadOptions().SetIterateUpperBound("t")*/))
        {
            iterator.Seek(keyPrefix);

            int count = 0;
            while (iterator.Valid())
            {
                string key = iterator.StringKey();
                string value = iterator.StringValue();
                //print(key, value);
                if (key.StartsWith(keyPrefix))
                {
                    count++;
                }
                else
                {
                    break;
                }
                iterator.Next();
            }
            print($"count: {count}");
        }
    }

    public static void Find(RocksDbSharp.RocksDb db, string from, string to)
    {

        using (var iterator = db.NewIterator(/*readOptions: new ReadOptions().SetIterateUpperBound("t")*/))
        {
            iterator.Seek(from);

            int count = 0;
            while (iterator.Valid())
            {
                string key = iterator.StringKey();
                string value = iterator.StringValue();

                if (key == to)
                {
                    count++;
                    print(key);
                    break;
                }
                else
                {
                    count++;
                    print(key);
                }

                iterator.Next();
            }
            print($"count: {count}");
        }
    }

}
