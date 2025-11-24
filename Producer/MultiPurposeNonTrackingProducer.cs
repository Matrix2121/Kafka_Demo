using Confluent.Kafka;
using System.Text.Json;

class MultiPurposeNonTrackingProducer
{
    private static readonly string BOOTSTRAP_SERVERS = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS");

    static public async Task Start()
    {
        Console.Title = "Producer";
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

        var OrdersTask = Task.Run(() => OrdersProducer(OrdersAndWalletConfig));
        var WalletUpdateTask = Task.Run(() => WalletUpdateProducer(OrdersAndWalletConfig));
        var MarketPriceUpdateTask = Task.Run(() => MarketPriceUpdateProducer(MarketPriceUpdateConfig));
        var NotificationsTask = Task.Run(() => NotificationsProducer(NotificationsConfig));

        await Task.WhenAll(OrdersTask, WalletUpdateTask, MarketPriceUpdateTask, NotificationsTask);
    }

    static private async Task MarketPriceUpdateProducer(ProducerConfig config)
    {
        Random rand = new Random();
        try
        {
            using (var producer = new ProducerBuilder<string, string>(config).Build())
            {
                string topic = "market.prices.raw";
                var pairs = new[] { "BTC-USD", "ETH-USD", "SOL-USD" };

                while (true)
                {
                    var symbol = pairs[rand.Next(pairs.Length)];
                    var message = new Message<string, string>
                    {
                        Key = symbol,
                        Value = JsonSerializer.Serialize(new { Price = rand.Next(3000, 60000) })
                    };

                    producer.Produce(topic, message);
                    Console.WriteLine("Price Update Executed");
                    await Task.Delay(rand.Next(100, 1000));
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Market Producer Error: {ex.Message}");
        }
    }

    static private async Task NotificationsProducer(ProducerConfig config)
    {
        Random rand = new Random();
        try
        {
            using (var producer = new ProducerBuilder<Null, string>(config).Build())
            {
                string topic = "notifications.marketing.blast";
                var cryto = new[] { "BTC", "ETH", "SOL" };

                while (true)
                {
                    for (int i = 0; i < 100; i++)
                    {
                        var message = new Message<Null, string>
                        {
                            Value = JsonSerializer.Serialize(new { Crypto = cryto[rand.Next(cryto.Length)], UserId = rand.Next(1000) })
                        };

                        producer.Produce(topic, message);
                    }

                    Console.WriteLine("100 Notifications Sent");
                    await Task.Delay(rand.Next(4000, 10000));
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Ntifications Producer Error: {ex.Message}");
        }
    }

    static private async Task OrdersProducer(ProducerConfig config)
    {
        Random rand = new Random();
        try
        {
            using (var producer = new ProducerBuilder<int, string>(config).Build())
            {
                string topic = "orders.requests.incoming";
                var cryto = new[] { "BTC", "ETH", "SOL" };

                while (true)
                {
                    int userId = rand.Next(1, 12);

                    var message = new Message<int, string>
                    {
                        Key = userId,
                        Value = JsonSerializer.Serialize(new { Crypto = cryto[rand.Next(cryto.Length)], UserId = userId, Amount = rand.Next(1, 100) })
                    };

                    producer.Produce(topic, message);
                    Console.WriteLine("Order Executed");
                    await Task.Delay(rand.Next(100, 1000));
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Order Producer Error: {ex.Message}");
        }
    }

    static private async Task WalletUpdateProducer(ProducerConfig config)
    {
        Random rand = new Random();
        try
        {
            using (var producer = new ProducerBuilder<int, string>(config).Build())
            {
                string topic = "wallet.balance.updates";

                while (true)
                {
                    int userId = rand.Next(1, 12);

                    var message = new Message<int, string>
                    {
                        Key = userId,
                        Value = JsonSerializer.Serialize(new { UserId = userId, Amount = rand.Next(1, 1000) })
                    };

                    producer.Produce(topic, message);
                    Console.WriteLine("Wallet Update Executed");
                    await Task.Delay(rand.Next(100, 1000));
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Wallet Producer Error: {ex.Message}");
        }
    }
}