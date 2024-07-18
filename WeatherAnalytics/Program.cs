using WeatherAnalytics;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
builder.Services.AddControllers();
builder.Services.Configure<PulsarSettings>(builder.Configuration.GetSection("Pulsar"));
builder.Services.Configure<ElasticSearchSettings>(builder.Configuration.GetSection("ElasticSearch"));
builder.Services.AddSingleton<EventProcessor>();
builder.Services.AddSingleton<PulsarProducer>(); // Register PulsarProducer as a singleton
builder.Services.AddHostedService(provider => provider.GetRequiredService<PulsarProducer>()); // Register PulsarProducer as a hosted service

// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();
app.UseAuthorization();
app.MapControllers();
app.Run();
