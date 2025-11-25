using Confluent.Kafka;

namespace Kafka_Consumers
{
    class realtime_prices_stream_ui
    {
        private static readonly string BOOTSTRAP_SERVERS = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS");
        private const int CONSUME_TIMEOUT_MS = 100;
        private const int IDLE_DELAY_MS = 2;
        private const int CONSUMER_COUNT = 1;
        private static CancellationTokenSource _cancellationTokenSource;

        static public async Task Start()
        {
            Console.Title = "Realtime prices stream service";
            _cancellationTokenSource = new CancellationTokenSource();

            Console.CancelKeyPress += (sender, e) =>
            {
                e.Cancel = true;
                Console.WriteLine("\nShutting down prices stream service...");
                _cancellationTokenSource.Cancel();
            };

            var RealtimePricesStreamConfig = new ConsumerConfig
            {
                BootstrapServers = BOOTSTRAP_SERVERS,
                GroupId = "realtime-prices-stream-ui",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false,
                SessionTimeoutMs = 10000,
                HeartbeatIntervalMs = 3000
            };


            var tasks = new List<Task>();
            for (int i = 1; i <= CONSUMER_COUNT; i++)
            {
                int consumerId = i;
                tasks.Add(Task.Run(async () => await RealtimePricesStreamConsumer(RealtimePricesStreamConfig, consumerId, _cancellationTokenSource.Token)));
            }

            try
            {
                await Task.WhenAll(tasks);
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("All consumers stopped successfully.");
            }
            finally
            {
                _cancellationTokenSource?.Dispose();
                Console.WriteLine("Realtime prices stream service has been shut down. Press any key to exit.");
                Console.ReadKey();
            }
        }

        static private async Task RealtimePricesStreamConsumer(ConsumerConfig config, int consumerId, CancellationToken cancellationToken)
        {
            using var consumer = new ConsumerBuilder<string, string>(config).Build();

            consumer.Subscribe(new List<string> {
                    "market.prices.raw"
                });

            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        var result = consumer.Consume(TimeSpan.FromMilliseconds(CONSUME_TIMEOUT_MS));

                        if (result == null)
                        {
                            await Task.Delay(IDLE_DELAY_MS, cancellationToken);
                            continue;
                        }

                        ConsumerWork(result, consumerId);
                        consumer.Commit(result);
                    }
                    catch (ConsumeException ex)
                    {
                        Console.WriteLine($"[Consumer {consumerId}] Consume error: {ex.Error.Reason}");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine($"[Consumer {consumerId}] Cancellation requested");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[Consumer {consumerId}] Fatal error: {ex.Message}");
            }
            finally
            {
                try
                {
                    consumer.Close();
                    Console.WriteLine($"[Consumer {consumerId}] Closed successfully");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[Consumer {consumerId}] Error closing: {ex.Message}");
                }
            }
        }

        private static void ConsumerWork(ConsumeResult<string, string> result, int consumerId)
        {
            if (result.Topic == "market.prices.raw")
            {
                Console.WriteLine($"[Consumer: {consumerId}][Partition {{{result.Partition.Value}}}] Price Update - {result.Message.Key}: {result.Message.Value}");
            }
        }
    }
}
