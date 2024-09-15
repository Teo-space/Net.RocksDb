using DotnetRocksDb.Tests.Domain;
using RocksDb.Extensions;
using RocksDb.Extensions.Helpers;
using RocksDbSharp;

namespace DotnetRocksDb.Tests.TestsWithBinaryKeys;

public static class SimpleBinaryTests
{
    public static IReadOnlyCollection<Message> PrepareMessages()
    {
        print($"Preparing Messages");
        List<Message> messages = new List<Message>();

        DateTime date = DateTime.Parse("2000.01.01");
        int dateIncrement = 0;

        for (long serverId = 0; serverId < 1000; serverId++)
        {
            for (long channelId = 0; channelId < 10; channelId++)
            {
                for (long messageId = 0; messageId < 1000; messageId++)
                {
                    var message = new Message()
                    {
                        Key = new MessageKey(serverId, channelId, messageId),
                        Date = date.AddSeconds(dateIncrement++),
                        Text = "Test Message"
                    };

                    messages.Add(message);
                }
            }
        }
        print($"Preparing Messages Done");
        return messages;
    }

    public static IReadOnlyCollection<KeyValuePair<byte[], byte[]>> GetMessagesBinary(IReadOnlyCollection<Message> messages)
    {
        print($"Preparing Messages Binary");
        List<KeyValuePair<byte[], byte[]>> data = new List<KeyValuePair<byte[], byte[]>>();

        foreach (Message message in messages)
        {
            data.Add(new KeyValuePair<byte[], byte[]>(message.Key, message.Bytes()));
        }
        print($"Preparing Messages Binary Done");

        return data;
    }
    /*
[2024.09.15:14:38.30.817] [1 ms]        try open:
[2024.09.15:14:38.30.885] [68 ms]       Success!!
[2024.09.15:14:38.30.891] [5 ms]        Preparing Messages
[2024.09.15:14:38.32.712] [1820 ms]     Preparing Messages Done
[2024.09.15:14:38.32.713] [1 ms]        Preparing Messages Binary
[2024.09.15:14:38.45.628] [12915 ms]    Preparing Messages Binary Done
[2024.09.15:14:38.45.629] [0 ms]        Start Put
[2024.09.15:14:39.40.295] [54666 ms]    End Put
[2024.09.15:14:39.40.297] [0 ms]        Get Range Keys
[2024.09.15:14:39.40.299] [1 ms]        Done
[2024.09.15:14:39.40.300] [0 ms]        Start CompactRange
[2024.09.15:14:39.43.805] [3505 ms]     End CompactRange
[2024.09.15:14:39.43.806] [0 ms]        Start Flush
[2024.09.15:14:39.43.808] [1 ms]        End Flush
    */
    public static void SaveMessages(RocksDbSharp.RocksDb db)
    {
        var messages = PrepareMessages();
        var data = GetMessagesBinary(messages);
        print($"Start Put");
        foreach (var d in data)
        {
            db.Put(d.Key, d.Value);
        }
        print($"End Put");

        db.CompactDatabaseBinary();
    }
    /*
[2024.09.15:14:36.53.058] [1 ms]        try open:
[2024.09.15:14:36.53.127] [69 ms]       Success!!
[2024.09.15:14:36.53.133] [5 ms]        Preparing Messages
[2024.09.15:14:36.55.003] [1869 ms]     Preparing Messages Done
[2024.09.15:14:36.55.005] [1 ms]        Preparing Messages Binary
[2024.09.15:14:37.08.119] [13113 ms]    Preparing Messages Binary Done
[2024.09.15:14:37.08.120] [0 ms]        Prepare Batch
[2024.09.15:14:37.09.919] [1800 ms]     End Prepare Batch
[2024.09.15:14:37.09.921] [1 ms]        Start Put Batch
[2024.09.15:14:37.17.102] [7181 ms]     End Put Batch
[2024.09.15:14:37.17.104] [0 ms]        Get Range Keys
[2024.09.15:14:37.17.106] [1 ms]        Done
[2024.09.15:14:37.17.106] [0 ms]        Start CompactRange
[2024.09.15:14:37.20.689] [3583 ms]     End CompactRange
[2024.09.15:14:37.20.691] [0 ms]        Start Flush
[2024.09.15:14:37.20.692] [1 ms]        End Flush
    */
    public static void SaveMessagesBatch(RocksDbSharp.RocksDb db)
    {
        var messages = PrepareMessages();
        var data = GetMessagesBinary(messages);

        print($"Prepare Batch");
        WriteBatch writeBatch = new WriteBatch();
        foreach (var d in data)
        {
            writeBatch.Put(d.Key, d.Value);
        }
        print($"End Prepare Batch");

        print($"Start Put Batch");
        db.Write(writeBatch);
        print($"End Put Batch");

        db.CompactDatabaseBinary();
    }



    public static void BinaryIteratorPrefixCount(RocksDbSharp.RocksDb db, long serverId = 500)
    {
        using (var iterator = db.NewIterator())
        {
            byte[] keyPrefix = BitConverter.GetBytes(serverId);
            iterator.Seek(keyPrefix);

            int count = 0;
            while (iterator.Valid())
            {
                byte[] rowKey = iterator.Key();
                if (rowKey.Length > 0 && rowKey.Length >= keyPrefix.Length)
                {
                    if (keyPrefix.SequenceEqual(rowKey.Take(keyPrefix.Length)))
                    {
                        count++;
                    }
                    else break;
                }
                else break;


                iterator.Next();
            }
            print($"count: {count}");
        }
    }


    public static void BinaryIteratorCompositePrefixCount(RocksDbSharp.RocksDb db, params long[] keys)
    {
        using (var iterator = db.NewIterator())
        {
            byte[] keyPrefix = NumericConverter.ToBytes(keys);
            iterator.Seek(keyPrefix);

            int count = 0;
            while (iterator.Valid())
            {
                byte[] rowKey = iterator.Key();
                if (rowKey.Length > 0 && rowKey.Length >= keyPrefix.Length)
                {
                    if (keyPrefix.SequenceEqual(rowKey.Take(keyPrefix.Length)))
                    {
                        count++;
                    }
                    else break;
                }
                else break;


                iterator.Next();
            }
            print($"count: {count}");
        }
    }


}
