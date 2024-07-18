using DotPulsar;
using DotPulsar.Abstractions;
using DotPulsar.Extensions;
using Microsoft.Extensions.Options;
using System;
using System.Buffers;
using System.Threading.Tasks;

namespace WeatherAnalytics
{
    public class DataIngestionService
    {
        private readonly IPulsarClient _client;
        private readonly string _topic;
        private readonly string _subscriptionName;

        public DataIngestionService(IOptions<PulsarSettings> settings)
        {
            _client = PulsarClient.Builder()
                .ServiceUrl(new Uri(settings.Value.ServiceUrl))
                .Build();
            _topic = settings.Value.Topic;
            _subscriptionName = settings.Value.SubscriptionName;
        }

        public async Task IngestDataAsync()
        {
            var consumer = _client.NewConsumer()
                .Topic(_topic)
                .SubscriptionName(_subscriptionName)
                .Create();

            var producer = _client.NewProducer()
                .Topic("persistent://public/default/weather")
                .Create();

            await foreach (var message in consumer.Messages())
            {
                await producer.Send(message.Data);
                await consumer.Acknowledge(message);
            }
        }
    }

    public class PulsarSettings
    {
        public string ServiceUrl { get; set; }
        public string Topic { get; set; }
        public string SubscriptionName { get; set; }
    }
}
