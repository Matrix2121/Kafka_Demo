using Confluent.Kafka;
using System.Text.Json;

class MultiPurposeTrackingProducer
{
    private static readonly string BOOTSTRAP_SERVERS = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS");
    private static CancellationTokenSource _cancellationTokenSource;

    static public async Task Start()
    {
        Console.Title = "Producer";
        _cancellationTokenSource = new CancellationTokenSource();

        Console.CancelKeyPress += (sender, e) =>
        {
            e.Cancel = true;
            Console.WriteLine("\nShutting down producers...");
            _cancellationTokenSource.Cancel();
        };

        var OrdersAndWalletConfig = new ProducerConfig
        {
            BootstrapServers = BOOTSTRAP_SERVERS,
            Acks = Acks.All,
            LingerMs = 0,
            EnableIdempotence = true
        };

        var MarketPriceUpdateConfig = new ProducerConfig
        {
            BootstrapServers = BOOTSTRAP_SERVERS,
            Acks = Acks.Leader,
            LingerMs = 10,
            CompressionType = CompressionType.Lz4,
            EnableIdempotence = false
        };
        var NotificationsConfig = new ProducerConfig
        {
            BootstrapServers = BOOTSTRAP_SERVERS,
            Acks = Acks.None,
            LingerMs = 200,
            CompressionType = CompressionType.Lz4,
            EnableIdempotence = false
        };

        var OrdersTask = Task.Run(() => OrdersProducer(OrdersAndWalletConfig, _cancellationTokenSource.Token));
        var WalletUpdateTask = Task.Run(() => WalletUpdateProducer(OrdersAndWalletConfig, _cancellationTokenSource.Token));
        var MarketPriceUpdateTask = Task.Run(() => MarketPriceUpdateProducer(MarketPriceUpdateConfig, _cancellationTokenSource.Token));
        var NotificationsTask = Task.Run(() => NotificationsProducer(NotificationsConfig, _cancellationTokenSource.Token));

        try
        {
            await Task.WhenAll(OrdersTask, WalletUpdateTask, MarketPriceUpdateTask, NotificationsTask);
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("All producers stopped successfully.");
        }
        finally
        {
            _cancellationTokenSource?.Dispose();
            Console.WriteLine("Producer service has been shut down. Press any key to exit.");
            Console.ReadKey();
        }

    }

    static private async Task MarketPriceUpdateProducer(ProducerConfig config, CancellationToken cancellationToken)
    {
        Random rand = new Random();
        try
        {
            using (var producer = new ProducerBuilder<string, string>(config).Build())
            {
                string topic = "market.prices.raw";
                var pairs = new[] { "BTC-USD", "ETH-USD", "SOL-USD" };

                while (!cancellationToken.IsCancellationRequested)
                {
                    var symbol = pairs[rand.Next(pairs.Length)];
                    var message = new Message<string, string>
                    {
                        Key = symbol,
                        Value = JsonSerializer.Serialize(new { Price = rand.Next(3000, 60000) })
                    };

                    var result = await producer.ProduceAsync(topic, message);
                    Console.WriteLine($"[Partition {{{result.Partition.Value}}}] Price Update Executed");
                    await Task.Delay(rand.Next(100, 1000));
                }

                producer.Flush(TimeSpan.FromSeconds(10));
                Console.WriteLine("Market Price Producer closed successfully");
            }
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("Market Price Producer: Shutdown requested");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Market Producer Error: {ex.Message}");
        }
    }

    static private async Task NotificationsProducer(ProducerConfig config, CancellationToken cancellationToken)
    {
        Random rand = new Random();
        try
        {
            using (var producer = new ProducerBuilder<Null, string>(config).Build())
            {
                string topic = "notifications.marketing.blast";
                var crypto = new[] { "BTC", "ETH", "SOL" };
                List<long> partitionsHits = new List<long>();
                List<long> uniquePartitions = new List<long>();

                while (!cancellationToken.IsCancellationRequested)
                {
                    for (int i = 0; i < 100 && !cancellationToken.IsCancellationRequested; i++)
                    {
                        var message = new Message<Null, string>
                        {
                            Value = JsonSerializer.Serialize(new { Crypto = crypto[rand.Next(crypto.Length)], UserId = rand.Next(1000) })
                        };

                        var result = await producer.ProduceAsync(topic, message);
                        partitionsHits.Add(result.Partition.Value);
                    }

                    uniquePartitions.AddRange(partitionsHits.Distinct());
                    Console.WriteLine($"[Partitions: {{{string.Join(", ", uniquePartitions)}}}] {partitionsHits.Count} Notifications Sent");
                    partitionsHits.Clear();
                    uniquePartitions.Clear();
                    await Task.Delay(rand.Next(4000, 10000));
                }

                producer.Flush(TimeSpan.FromSeconds(10));
                Console.WriteLine("Notifications Producer closed successfully");
            }
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("Notifications Producer: Shutdown requested");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Notifications Producer Error: {ex.Message}");
        }
    }

    static private async Task OrdersProducer(ProducerConfig config, CancellationToken cancellationToken)
    {
        Random rand = new Random();
        try
        {
            using (var producer = new ProducerBuilder<int, string>(config).Build())
            {
                string topic = "orders.requests.incoming";
                var cryto = new[] { "BTC", "ETH", "SOL" };

                while (!cancellationToken.IsCancellationRequested)
                {
                    int userId = rand.Next(1, 12);

                    var message = new Message<int, string>
                    {
                        Key = userId,
                        Value = JsonSerializer.Serialize(new { Crypto = cryto[rand.Next(cryto.Length)], UserId = userId, Amount = rand.Next(1, 100) })
                    };

                    var result = await producer.ProduceAsync(topic, message);
                    Console.WriteLine($"[Partition {{{result.Partition.Value}}}] Order Executed");
                    await Task.Delay(rand.Next(100, 1000));
                }

                producer.Flush(TimeSpan.FromSeconds(10));
                Console.WriteLine("Orders Producer closed successfully");
            }
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("Orders Producer: Shutdown requested");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Orders Producer Error: {ex.Message}");
        }
    }

    static private async Task WalletUpdateProducer(ProducerConfig config, CancellationToken cancellationToken)
    {
        Random rand = new Random();
        try
        {
            using (var producer = new ProducerBuilder<int, string>(config).Build())
            {
                string topic = "wallet.balance.updates";

                while (!cancellationToken.IsCancellationRequested)
                {
                    int userId = rand.Next(1, 12);

                    var message = new Message<int, string>
                    {
                        Key = userId,
                        Value = JsonSerializer.Serialize(new { UserId = userId, Amount = rand.Next(1, 1000) })
                    };

                    var result = await producer.ProduceAsync(topic, message);
                    Console.WriteLine($"[Partition {{{result.Partition.Value}}}] Wallet Update Executed");
                    await Task.Delay(rand.Next(100, 1000));
                }

                producer.Flush(TimeSpan.FromSeconds(10));
                Console.WriteLine("Wallet Update Producer closed successfully");
            }
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("Wallet Update Producer: Shutdown requested");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Wallet Update Producer Error: {ex.Message}");
        }
    }
}