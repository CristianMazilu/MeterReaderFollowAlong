using Grpc.Net.Client;
using MeterReaderWeb.Services;
using MeterReaderWeb.Services;
using Grpc.Net.Client;
using Google.Protobuf.WellKnownTypes;

namespace MeterReaderClient
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly IConfiguration _config;
        private readonly ReadingFactory factory;
        private MeterReadingService.MeterReadingServiceClient _client = null;

        public Worker(ILogger<Worker> logger, IConfiguration config, ReadingFactory factory)
        {
            _logger = logger;
            this._config = config;
            this.factory = factory;
        }

        protected MeterReadingService.MeterReadingServiceClient Client
        {
            get
            {
                if (_client == null)
                {
                    var channel = GrpcChannel.ForAddress(_config["Service:ServerUrl"]);
                    _client = new MeterReadingService.MeterReadingServiceClient(channel);
                }

                return _client;
            }
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var counter = 0;
            var customerId = _config.GetValue<int>("service:CustomerId");

            while (!stoppingToken.IsCancellationRequested)
            {
                counter++;

                if (counter % 10 == 0)
                {
                    Console.WriteLine("Sending Diagnostics");

                    var stream = Client.SendDiagnostics();

                    for (var i = 0; i < 5; i++)
                    {
                        var reading = await factory.Generate(customerId);

                        await stream.RequestStream.WriteAsync(reading);
                    }

                    await stream.RequestStream.CompleteAsync();
                }

                _logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);

                var pkt = new ReadingPacket()
                {
                    Successful = ReadingStatus.Success,
                    Notes = "This is our test."
                };

                for (var x = 0; x < 5; ++x)
                {
                    pkt.Readings.Add(await factory.Generate(customerId));
                }

                var result = await Client.AddReadingAsync(pkt);
                if (result.Success == ReadingStatus.Success)
                {
                    _logger.LogInformation("Successfully sent");
                }
                else
                {
                    _logger.LogInformation("Failed to send");
                }

                await Task.Delay(_config.GetValue<int>("Service:DelayInterval"), stoppingToken);
            }
        }
    }
}
