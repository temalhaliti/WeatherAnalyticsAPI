﻿using DotPulsar;
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
        private readonly string _inputTopic;
        private readonly string _outputTopic;
        private readonly string _subscriptionName;
        private readonly ElasticClient _elasticClient;

        public EventProcessor(IOptions<PulsarSettings> pulsarSettings, IOptions<ElasticSearchSettings> elasticSettings)
        {
            _client = PulsarClient.Builder()
                .ServiceUrl(new Uri(pulsarSettings.Value.ServiceUrl))
                .Build();
            _inputTopic = pulsarSettings.Value.Topic;
            _outputTopic = pulsarSettings.Value.ProcessedTopic;
            _subscriptionName = pulsarSettings.Value.SubscriptionName;

            var settings = new ConnectionSettings(new Uri(elasticSettings.Value.Url))
                .DefaultIndex(elasticSettings.Value.Index);
            _elasticClient = new ElasticClient(settings);
        }

        public async Task ProcessEventsAsync()
        {
            var consumer = _client.NewConsumer()
                .Topic(_inputTopic)
                .SubscriptionName(_subscriptionName)
                .SubscriptionType(SubscriptionType.Shared)
                .Create();

            var producer = _client.NewProducer()
                .Topic(_outputTopic)
                .Create();

            Console.WriteLine("Event Processor started. Press any key to exit...");

            await foreach (var message in consumer.Messages())
            {
                var weatherData = JsonConvert.DeserializeObject<WeatherData>(Encoding.UTF8.GetString(message.Data.ToArray()));

                // Enrich and process data
                weatherData.ProcessedTimestamp = DateTime.UtcNow;
                double temperatureCelsius = double.Parse(weatherData.Temperature);
                double temperatureFahrenheit = (temperatureCelsius * 9 / 5) + 32;
                weatherData.Description = $"Temperature: {temperatureCelsius}°C\nTemperature: {temperatureFahrenheit}°F\nTimestamp: {weatherData.Timestamp:F}";
                weatherData.FormattedTimestamp = weatherData.Timestamp.ToString("F");

                // Index data in Elasticsearch
                await _elasticClient.IndexDocumentAsync(weatherData);

                Console.WriteLine($"Indexed data in Elasticsearch: {JsonConvert.SerializeObject(weatherData)}");

                // Produce enriched data to the output topic
                var enrichedMessage = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(weatherData));
                await producer.Send(new ReadOnlySequence<byte>(enrichedMessage));

                Console.WriteLine($"Produced enriched message to output topic: {JsonConvert.SerializeObject(weatherData)}");

                await consumer.Acknowledge(message);
                Console.WriteLine($"Acknowledged message: {JsonConvert.SerializeObject(weatherData)}");
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
        public string Description { get; set; }
        public string FormattedTimestamp { get; set; }
    }
}
