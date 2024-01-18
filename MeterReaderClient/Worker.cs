using Grpc.Net.Client;
using MeterReaderWeb.Services;
using MeterReaderWeb.Services;
using Grpc.Net.Client;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Microsoft.Extensions.Logging;

namespace MeterReaderClient
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly IConfiguration _config;
        private readonly ReadingFactory factory;
        private readonly ILoggerFactory loggerFactory;
        private MeterReadingService.MeterReadingServiceClient _client = null;
        private string token;
        private DateTime expriration = DateTime.MinValue;

        public Worker(ILogger<Worker> logger,
                    IConfiguration config,
                    ReadingFactory factory,
                    ILoggerFactory loggerFactory)
        {
            _logger = logger;
            this._config = config;
            this.factory = factory;
            this.loggerFactory = loggerFactory;
        }

        protected MeterReadingService.MeterReadingServiceClient Client
        {
            get
            {
                if (_client == null)
                {
                    var opt = new GrpcChannelOptions()
                    {
                        LoggerFactory = loggerFactory,
                    };

                    var channel = GrpcChannel.ForAddress(_config["Service:ServerUrl"], opt);
                    _client = new MeterReadingService.MeterReadingServiceClient(channel);
                }

                return _client;
            }
        }

        protected bool NeedsLogin() => string.IsNullOrWhiteSpace(token) || expriration > DateTime.UtcNow;

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var counter = 0;
            var customerId = _config.GetValue<int>("service:CustomerId");

            while (!stoppingToken.IsCancellationRequested)
            {
                counter++;

                /*if (counter % 10 == 0)
                {
                    Console.WriteLine("Sending Diagnostics");

                    var stream = Client.SendDiagnostics();

                    for (var i = 0; i < 5; i++)
                    {
                        var reading = await factory.Generate(customerId);

                        await stream.RequestStream.WriteAsync(reading);
                    }

                    await stream.RequestStream.CompleteAsync();
                }*/

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

                try
                {
                    if (!NeedsLogin() || await GenerateToken())
                    {
                        var headers = new Metadata();
                        headers.Add("Authorization", $"Bearer {token}");

                        var result = await Client.AddReadingAsync(pkt, headers: headers);
                        if (result.Success == ReadingStatus.Success)
                        {
                            _logger.LogInformation("Successfully sent");
                        }
                        else
                        {
                            _logger.LogInformation("Failed to send");
                        }
                    }
                }
                catch (RpcException ex)
                {
                    if (ex.StatusCode == StatusCode.OutOfRange)
                    {
                        _logger.LogError($"{ex.Trailers.Aggregate(string.Empty, (result, f) => result + $"Key: {f.Key}, Value: {f.Value}\n")}");
                    }
                    _logger.LogError($"Exception thrown{ex}");
                }

                

                await Task.Delay(_config.GetValue<int>("Service:DelayInterval"), stoppingToken);
            }
        }

        private async Task<bool> GenerateToken()
        {
            var request = new TokenRequest()
            {
                Username = _config["Service:Username"],
                Password = _config["Service:Password"],
            };

            var response = await Client.CreateTokenAsync(request);

            if (response.Success)
            {
                token = response.Token;
                expriration = response.Expiration.ToDateTime();

                return true;
            }

            return false;
        }
    }
}
