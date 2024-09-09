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
    PerfomanceTests.RunReadParallelRepeatReadOnly(db, documentIds, 1000);//result 100 reads in 1 m\s

    /*
    print($"Start Read");
    var documentJson = db.Get("01J78XCZNH9V0BF0N0J6CYHQTD");
    print($"End Read");
    var document = Document.From(documentJson);
    print(document);
    */
    Console.ReadLine();
}


