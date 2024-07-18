using DotPulsar;
using DotPulsar.Abstractions;
using DotPulsar.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using System;
using System.Buffers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace WeatherAnalytics
{
    public class PulsarProducer : IHostedService, IDisposable
    {
        private readonly IPulsarClient _client;
        private readonly string _topic;
        private IProducer<ReadOnlySequence<byte>> _producer;

        public PulsarProducer(IOptions<PulsarSettings> settings)
        {
            _client = PulsarClient.Builder().ServiceUrl(new Uri(settings.Value.ServiceUrl)).Build();
            _topic = settings.Value.Topic;
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            _producer = _client.NewProducer()
                .Topic(_topic)
                .Create();
            return Task.CompletedTask;
        }

        public async Task SendMessageAsync(string message)
        {
            var data = Encoding.UTF8.GetBytes(message);
            var sequence = new ReadOnlySequence<byte>(data);
            await _producer.Send(sequence);
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            Dispose();
            return Task.CompletedTask;
        }

        public void Dispose()
        {
            _client?.DisposeAsync().AsTask().Wait();
        }
    }
}
