using DotPulsar;
using DotPulsar.Abstractions;
using DotPulsar.Extensions;
using Microsoft.Extensions.Options;
using Nest;
using Newtonsoft.Json;
using System;
using System.Buffers;
using System.Text;
using System.Threading.Tasks;

namespace WeatherAnalytics
{
    public class EventProcessor
    {
        private readonly IPulsarClient _client;
        private readonly string _topic;
        private readonly string _subscriptionName;
        private readonly ElasticClient _elasticClient;

        public EventProcessor(IOptions<PulsarSettings> pulsarSettings, IOptions<ElasticSearchSettings> elasticSettings)
        {
            _client = PulsarClient.Builder()
                .ServiceUrl(new Uri(pulsarSettings.Value.ServiceUrl))
                .Build();
            _topic = pulsarSettings.Value.Topic;
            _subscriptionName = pulsarSettings.Value.SubscriptionName;

            var settings = new ConnectionSettings(new Uri(elasticSettings.Value.Url))
                .DefaultIndex(elasticSettings.Value.Index);
            _elasticClient = new ElasticClient(settings);
        }

        public async Task ProcessEventsAsync()
        {
            var consumer = _client.NewConsumer()
                .Topic(_topic)
                .SubscriptionName(_subscriptionName)
                .SubscriptionType(SubscriptionType.Shared)
                .Create();

            Console.WriteLine("Event Processor started. Press any key to exit...");

            await foreach (var message in consumer.Messages())
            {
                var weatherData = JsonConvert.DeserializeObject<WeatherData>(Encoding.UTF8.GetString(message.Data.ToArray()));

                // Enrich and process data
                weatherData.ProcessedTimestamp = DateTime.UtcNow;

                // Index data in Elasticsearch
                await _elasticClient.IndexDocumentAsync(weatherData);

                Console.WriteLine($"Indexed data in Elasticsearch: {JsonConvert.SerializeObject(weatherData)}");

                await consumer.Acknowledge(message);
                Console.WriteLine($"Consumed and acknowledged message: {JsonConvert.SerializeObject(weatherData)}");
            }
        }
    }

    public class PulsarSettings
    {
        public string ServiceUrl { get; set; }
        public string Topic { get; set; }
        public string SubscriptionName { get; set; }
    }

    public class ElasticSearchSettings
    {
        public string Url { get; set; }
        public string Index { get; set; }
    }

    public class WeatherData
    {
        public string Temperature { get; set; }
        public DateTime Timestamp { get; set; }
        public DateTime ProcessedTimestamp { get; set; }
    }
}
