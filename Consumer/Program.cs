using Kafka_Consumers;
using DotNetEnv;

class Program
{

    static void Main(string[] args)
    {
        Env.Load();
        analytics_service.Start().GetAwaiter().GetResult();
    }
}