using DotPulsar;
using DotPulsar.Abstractions;
using DotPulsar.Extensions;
using Microsoft.Extensions.Options;
using System;
using System.Threading.Tasks;

namespace WeatherDataIngestion
{
    public class DataIngestionService
    {
        private readonly IPulsarClient _client;
        private readonly string _topic;

        public DataIngestionService(IOptions<PulsarSettings> pulsarSettings)
        {
            _client = PulsarClient.Builder()
                .ServiceUrl(new Uri(pulsarSettings.Value.ServiceUrl))
                .Build();
            _topic = pulsarSettings.Value.Topic;
        }

        public async Task IngestDataAsync()
        {
            var producer = _client.NewProducer().Topic(_topic).Create();
            var data = new { Temperature = "25.3", Timestamp = DateTime.UtcNow };
            var message = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(data);

            await producer.Send(message);
        }
    }

    public class PulsarSettings
    {
        public string ServiceUrl { get; set; }
        public string Topic { get; set; }
        public string SubscriptionName { get; set; }
    }
}
