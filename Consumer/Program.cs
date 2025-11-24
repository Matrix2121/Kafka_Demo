using Kafka_Consumers;
using DotNetEnv;

class Program
{

    static void Main(string[] args)
    {
        Env.Load();
        //analytics_service.Start().GetAwaiter().GetResult();
        //prices_data_warehouse_archiver.Start().GetAwaiter().GetResult();
        //realtime_prices_stream_ui.Start().GetAwaiter().GetResult();
        //notification_sender_worker.Start().GetAwaiter().GetResult();
        //order_execution_engine.Start().GetAwaiter().GetResult();
    }
}