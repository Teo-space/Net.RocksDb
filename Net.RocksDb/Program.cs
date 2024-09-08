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

//var documents = PrepareDocuments(documentIds, 10_000_000);

print($"try open:");
using (var db = RocksDb.Open(options, directory))
{
    print($"Success!!");
    //PerfomanceTests.RunWriteBatch(db, documents, documentIds);
    //db.Flush(new FlushOptions());
    //PerfomanceTests.RunRead(db, documentIds);
    //File.WriteAllLines(Path.Combine(directory, "documentIds.txt"), documentIds);

    documentIds = File.ReadAllLines(Path.Combine(directory, "documentIds.txt")).ToList();

    PerfomanceTests.RunReadParallelRepeat(db, documentIds);

    /*
    print($"Start Read");
    var documentJson = db.Get("01J78XCZNH9V0BF0N0J6CYHQTD");
    print($"End Read");
    var document = Document.From(documentJson);
    print(document);
    */
    Console.ReadLine();
}


