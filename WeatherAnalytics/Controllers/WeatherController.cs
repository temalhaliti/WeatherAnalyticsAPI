using Microsoft.AspNetCore.Mvc;

namespace WeatherAnalytics.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class WeatherController : ControllerBase
    {
        private readonly PulsarProducer _pulsarProducer;

        public WeatherController(PulsarProducer pulsarProducer)
        {
            _pulsarProducer = pulsarProducer;
        }

        [HttpPost]
        public async Task<IActionResult> Post([FromBody] WeatherData weatherData)
        {
            var message = Newtonsoft.Json.JsonConvert.SerializeObject(weatherData);
            await _pulsarProducer.SendMessageAsync(message);
            return Ok();
        }
    }

    public class WeatherData
    {
        public string Temperature { get; set; }
        public DateTime Timestamp { get; set; }
    }
}
