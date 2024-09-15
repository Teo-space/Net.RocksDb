using DotnetRocksDb.Tests.TestsWithBinaryKeys;
using RocksDbSharp;

//await StringTests();
await BinaryTests();


async Task BinaryTests()
{
    List<string> documentIds = new List<string>();

    var options = new DbOptions()
        .SetCreateIfMissing(true)
        .IncreaseParallelism(16)
        //.SetWalDir()
        ;

    string directory = Path.Combine(Directory.GetCurrentDirectory(), "DataBin");
    if (Directory.Exists(directory) == false) Directory.CreateDirectory(directory);

    print($"Selected Path: {directory}");
    print($"try open:");
    using var db = RocksDbSharp.RocksDb.Open(options, directory);
    print($"Success!!");

    /*
BinaryIteratorPrefixCount(db, 500L);
BinaryIteratorPrefixCount(db, 600L);
BinaryIteratorPrefixCount(db, 700L);
BinaryIteratorPrefixCount(db, 999L);
BinaryIteratorPrefixCount2(db, 500L, 5L);
*/
    SimpleBinaryTests.SaveMessages(db);
    //SimpleBinaryTests.SaveMessagesBatch(db);


    Console.ReadLine();
}

async Task StringTests()
{
    List<string> documentIds = new List<string>();

    var options = new DbOptions()
        .SetCreateIfMissing(true)
        .IncreaseParallelism(16)
        //.SetWalDir()
        ;

    string directory = Path.Combine(Directory.GetCurrentDirectory(), "Data");
    if (Directory.Exists(directory) == false) Directory.CreateDirectory(directory);

    print($"Selected Path: {directory}");
    print($"try open:");
    using (var db = RocksDbSharp.RocksDb.Open(options, directory))
    {
        print($"Success!!");

        {
            //var documents = PerfomanceTests.PrepareDocuments(documentIds, 100_000);
            //File.WriteAllLines(Path.Combine(directory, "documentIds.txt"), documentIds);
            //PerfomanceTests.RunWrite(db, documents); db.CompactDatabaseBinary();
            //PerfomanceTests.RunWriteParallel(db, documents); db.CompactDatabaseBinary();
            //PerfomanceTests.RunWriteBatch(db, documents); db.CompactDatabaseBinary();
        }
        {
            documentIds = File.ReadAllLines(Path.Combine(directory, "documentIds.txt")).ToList();
            //print($"documentIds: {documentIds.Count}");
            //PerfomanceTests.RunRead(db, documentIds);
            //PerfomanceTests.RunReadParallelRepeat(db, documentIds);
            //PerfomanceTests.RunReadParallelRepeatReadOnly(db, documentIds, 10);//result 100 reads in 1 m\s
        }


        {
            //01J7D6E5HAFV5G965RY3V77KRY
            //01J7D6E5HA08G5P2VFWSP4M0XH
            //FindAll(db, "01J7D6E5HAFV5G965RY3V77KRY");
            //FindAll(db, "01J7D6E5HA08G5P2VFWSP4M0XH");

            /*
            PerfomanceTests.FindAll(db, "100000.100");
            PerfomanceTests.FindAll(db, "10000.");
            PerfomanceTests.FindAll(db, "20000.");
            PerfomanceTests.FindAll(db, "30000.");
            PerfomanceTests.FindAll(db, "40000.");
            PerfomanceTests.FindAll(db, "50000.");
            */
        }

        Console.ReadLine();
    }

}