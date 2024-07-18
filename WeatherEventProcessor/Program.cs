using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System.Threading.Tasks;

namespace WeatherAnalytics
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            var host = CreateHostBuilder(args).Build();
            var processor = host.Services.GetRequiredService<EventProcessor>();
            await processor.ProcessEventsAsync();
        }

        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureAppConfiguration((context, config) =>
                {
                    config.AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                          .AddEnvironmentVariables();
                })
                .ConfigureServices((context, services) =>
                {
                    services.Configure<PulsarSettings>(context.Configuration.GetSection(nameof(PulsarSettings)));
                    services.Configure<ElasticSearchSettings>(context.Configuration.GetSection(nameof(ElasticSearchSettings)));
                    services.AddSingleton<EventProcessor>();
                });
    }
}
