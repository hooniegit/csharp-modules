class MultiThread
{
    public static void ThreadFunction(object arg)
    {
        Console.WriteLine(">>> thread is working...");

        List<Thread> threadList = new List<Thread>();

        string stringArg = (string)arg;
        threadList.Add(new Thread(() => PrintFunction(stringArg)));
        threadList.Add(new Thread(HelloFunction));

        foreach (var thread in threadList)
        {
            thread.Start();
        }

        foreach (var thread in threadList)
        {
            thread.Join();
        }

        Console.WriteLine(">>> thread has finished its work.");
    }

    static void PrintFunction(string value)
    {
        Console.WriteLine($"Hello, {value}!");
    }

    static void HelloFunction()
    {
        Console.WriteLine("Hello, World!");
    }
}

class Program
{
    static void Main()
    {
        MultiThread.ThreadFunction("Kafka");
    }
}
