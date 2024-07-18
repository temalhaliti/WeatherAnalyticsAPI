using DotPulsar;
using DotPulsar.Abstractions;
using DotPulsar.Extensions;
using Microsoft.Extensions.Options;
using System;
using System.Buffers;
using System.Text;
using System.Threading.Tasks;

namespace WeatherDataIngestion
{
    public class DataIngestionService
    {
        private readonly IPulsarClient _client;
        private readonly string _processedTopic;
        private readonly string _subscriptionName;

        public DataIngestionService(IOptions<PulsarSettings> pulsarSettings)
        {
            _client = PulsarClient.Builder()
                .ServiceUrl(new Uri(pulsarSettings.Value.ServiceUrl))
                .Build();
            _processedTopic = pulsarSettings.Value.ProcessedTopic;
            _subscriptionName = pulsarSettings.Value.SubscriptionName;
        }

        public async Task IngestDataAsync()
        {
            var consumer = _client.NewConsumer()
                .Topic(_processedTopic)
                .SubscriptionName(_subscriptionName)
                .SubscriptionType(SubscriptionType.Shared)
                .Create();

            Console.WriteLine("Data Ingestion Service started. Press any key to exit...");

            await foreach (var message in consumer.Messages())
            {
                Console.WriteLine($"Consumed message: {Encoding.UTF8.GetString(message.Data.ToArray())}");
                await consumer.Acknowledge(message);
            }

            Console.ReadKey();
        }
    }

    public class PulsarSettings
    {
        public string ServiceUrl { get; set; }
        public string Topic { get; set; }
        public string ProcessedTopic { get; set; }
        public string SubscriptionName { get; set; }
    }
}
