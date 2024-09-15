global using static Print;
using System.Diagnostics;

internal static class Print
{
    static Stopwatch stopwatch = new Stopwatch();
    static Print() => stopwatch.Start();

    private static void PrintTime()
    {
        var dateTime = DateTime.Now;
        {
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.Write("[");
            {
                Console.ForegroundColor = ConsoleColor.DarkYellow;
                Console.Write($"{dateTime.ToString("yyyy.MM.dd")}");
                Console.ForegroundColor = ConsoleColor.White;
                Console.Write(":");
                Console.ForegroundColor = ConsoleColor.DarkGreen;
                Console.Write($"{dateTime.ToString("HH:mm.ss")}");
                Console.ForegroundColor = ConsoleColor.White;
                Console.Write(".");
                Console.ForegroundColor = ConsoleColor.DarkBlue;
                Console.Write($"{dateTime.ToString("fff")}");
            }
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.Write("]");
            Console.Write(" ");
        }
        {
            Console.ForegroundColor = ConsoleColor.DarkCyan;
            Console.Write("[");
            Console.ForegroundColor = ConsoleColor.DarkBlue;
            Console.Write($"{stopwatch.ElapsedMilliseconds} ms");
            Console.ForegroundColor = ConsoleColor.DarkCyan;
            Console.Write("]");
            stopwatch.Restart();
        }
        Console.ForegroundColor = ConsoleColor.White;
        Console.Write("	");
    }

    public static void print(object? o = null, ConsoleColor color = ConsoleColor.White)
    {
        PrintTime();
        Console.ForegroundColor = color;

        Console.Write(o ?? string.Empty);
        Console.WriteLine();
        Console.ForegroundColor = ConsoleColor.White;
    }

    public static void print(string text, ConsoleColor color = ConsoleColor.White) => print(text as object, color);

    public static void print(params object[] args)
    {
        for (int i = 0; i < args.Length; i++)
        {
            print(args[i]);
        }
    }

    public static void print<T>(IEnumerable<T> args)
    {
        foreach (var arg in args)
        {
            print(arg);
        }
    }

}