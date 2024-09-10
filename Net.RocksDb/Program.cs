using Net.RocksDb.Tests;
using RocksDbSharp;

List<string> documentIds = new List<string>();

var options = new DbOptions()
    .SetCreateIfMissing(true)
    .IncreaseParallelism(16)
    //.SetWalDir()
    ;

string directory = Path.Combine(Directory.GetCurrentDirectory(), "Data");
if (Directory.Exists(directory) == false) Directory.CreateDirectory(directory);

print($"Selected Path: {directory}");

//var documents = PerfomanceTests.PrepareDocuments(documentIds, 10_000_000);
//File.WriteAllLines(Path.Combine(directory, "documentIds.txt"), documentIds);

print($"try open:");
using (var db = RocksDb.Open(options, directory))
{
    print($"Success!!");
    //PerfomanceTests.RunWriteBatch(db, documents, documentIds); 

    documentIds = File.ReadAllLines(Path.Combine(directory, "documentIds.txt")).ToList();
    //print($"documentIds: {documentIds.Count}");
    //PerfomanceTests.RunRead(db, documentIds);
    //PerfomanceTests.RunReadParallelRepeat(db, documentIds);
    //PerfomanceTests.RunReadParallelRepeatReadOnly(db, documentIds, 1000);//result 100 reads in 1 m\s

    using (var iterator = db.NewIterator(/*readOptions: new ReadOptions().SetIterateUpperBound("t")*/))
    {
        iterator.Seek("01J7BXRBJ");//01J7BXRBJQY49R9GGZSQ6Z1V23

        int count = 0;
        while (iterator.Valid() )
        {
            string key = iterator.StringKey();
            string value = iterator.StringValue();
            //print(key, value);
            if (key.StartsWith("01J7BXRBJ"))
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

    /*
    print($"Start Read");
    var documentJson = db.Get("01J78XCZNH9V0BF0N0J6CYHQTD");
    print($"End Read");
    var document = Document.From(documentJson);
    print(document);
    */
    Console.ReadLine();
}

var blockBasedTableOptions = new BlockBasedTableOptions()
                             // Use a bloom filter to help reduce read amplification on point lookups. 10 bits per key yields a
                             // ~1% false positive rate as per the RocksDB documentation. This builds one filter per SST, which
                             // means its optimized for not having a key.
                             .SetFilterPolicy(BloomFilterPolicy.Create(10, false))
                             // Use a hash index in SST files to speed up point lookup.
                             .SetIndexType(BlockBasedTableIndexType.Hash)
                             // Whether to use the whole key or a prefix of it (obtained through the prefix extractor below).
                             // Since the prefix extractor is a no-op, better performance is achieved by turning this off (i.e.
                             // setting it to true).
                             .SetWholeKeyFiltering(true);

