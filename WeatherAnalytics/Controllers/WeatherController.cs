using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;
using WeatherAnalytics;

[ApiController]
[Route("[controller]")]
public class WeatherController : ControllerBase
{
    private readonly PulsarProducer _producer;

    public WeatherController(PulsarProducer producer)
    {
        _producer = producer;
    }

    [HttpPost]
    public async Task<IActionResult> Post([FromBody] WeatherData data)
    {
        var message = JsonConvert.SerializeObject(data);
        await _producer.SendMessageAsync(message);
        return Ok();
    }
}
