using Confluent.Kafka;

class KafkaConsumer
{
    // consumer 변수 선언
    private IConsumer<Ignore, string> consumer;

    // Consumer 객체 생성
    public KafkaConsumer(string bootstrapServers, string groupId, string topic)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        consumer = new ConsumerBuilder<Ignore, string>(config).Build();
        consumer.Subscribe(topic);

        Console.WriteLine($">>> Now Consuming Topic {topic}"); // test
    }

    // Data Poll
    public void PollDatas()
    {
        Console.WriteLine("Consumer started. Press Ctrl+C to exit."); // test

        try
        {
            while (true)
            {
                var consumeResult = consumer.Consume();

                // topic: consumerResult.Message.Topic
                // key: consumerResult.Message.Key
                // value: consumerResult.Message.Value
                // partition: consumerResult.Message.Partition
                // offset: consumerResult.Message.Offset
                // timestamp consumerResult.Message.Timestamp.DateTime

                Tasks(consumeResult);
            }
        }
        catch (OperationCanceledException)
        {
        }
        finally
        {
            consumer.Close();
        }
    }

    // Thread 작업
    private void Tasks(ConsumeResult<Ignore, string> consumeResult)
    {
        Console.WriteLine(">>> Tasks Started");

        List<Thread> threadList = new List<Thread>();

        threadList.Add(new Thread(() => PrintResults(consumeResult)));
        threadList.Add(new Thread(PrintHello));

        foreach (var thread in threadList)
        {
            thread.Start();
        }

        foreach (var thread in threadList)
        {
            thread.Join();
        }

        Console.WriteLine(">>> Tasks Finished");
    }

    // 단일 Thread 함수
    private void PrintResults(ConsumeResult<Ignore, string> consumeResult)
    {
        Console.WriteLine($@"Received message: 
Key: {consumeResult.Message.Key}, 
Value: {consumeResult.Message.Value},
Topic: {consumeResult.Topic},
Message: {consumeResult.Message.Value}, 
Partition: {consumeResult.Partition}, 
Offset: {consumeResult.Offset},
Timestamp: {consumeResult.Message.Timestamp.UnixTimestampMs}");
    }

    // 단일 Thread 함수
    private void PrintHello()
    {
        Console.WriteLine("Hello, Kafka!");
    }

}

class Program
{
    static void Main()
    {
        KafkaConsumer kafkaConsumer = new KafkaConsumer(
            "localhost:9092",
            "demo",
            "test"
            );

        kafkaConsumer.PollDatas();
    }
}
