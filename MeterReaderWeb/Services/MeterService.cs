using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using MeterReaderLib;
using MeterReaderLib.Models;
using MeterReaderWeb.Data;
using MeterReaderWeb.Data.Entities;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Authorization;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;

namespace MeterReaderWeb.Services
{
    [Authorize(AuthenticationSchemes = JwtBearerDefaults.AuthenticationScheme)]
    public class MeterService : MeterReadingService.MeterReadingServiceBase
    {
        private readonly ILogger<MeterService> _logger;
        private readonly IReadingRepository _repository;
        private readonly JwtTokenValidationService tokenService;

        public MeterService(ILogger<MeterService> logger,
                            IReadingRepository repository,
                            JwtTokenValidationService tokenService)
        {
            this._logger = logger;
            this._repository = repository;
            this.tokenService = tokenService;
        }

        [AllowAnonymous]
        public override async Task<TokenResponse> CreateToken(TokenRequest request, ServerCallContext context)
        {
            var creds = new CredentialModel()
            {
                UserName = request.Username,
                Passcode = request.Password
            };

            var response = await tokenService.GenerateTokenModelAsync(creds);

            if (response.Success)
            {
                return new TokenResponse
                {
                    Token = response.Token,
                    Expiration = Timestamp.FromDateTime(response.Expiration),
                    Success = true
                };
            }

            return new TokenResponse
            {
                Success = false
            };
        }

        public override async Task<Empty> SendDiagnostics(IAsyncStreamReader<ReadingMessage> requestStream,
                                                    ServerCallContext context)
        {
            var theTask = Task.Run(async () =>
            {
                await foreach (var reading in requestStream.ReadAllAsync())
                {
                    _logger.LogInformation($"Received reading: {reading}");
                }
            });

            await theTask;

            return new Empty();
        }

        public override async Task<StatusMessage> AddReading(ReadingPacket request,
                                                            ServerCallContext context)
        {
            var result = new StatusMessage()
            {
                Success = ReadingStatus.Failure
            };

            if (request.Successful == ReadingStatus.Success)
            {
                try
                {
                    foreach (var r in request.Readings)
                    {
                        if (r.ReadingValue <= 1000)
                        {
                            _logger.LogDebug("Reading value below acceptable levels.");

                            var trailer = new Metadata()
                            {
                                { "BadValue", r.ReadingValue.ToString() },
                                { "Field", "ReadingValue" },
                                { "Message", "Reading is invalid" }
                            };

                            throw new RpcException(new Status(StatusCode.OutOfRange, "Value"), trailer);
                        }

                        // Save to the database
                        var reading = new MeterReading()
                        {
                            Value = r.ReadingValue,
                            ReadingDate = r.ReadingTime.ToDateTime(),
                            CustomerId = r.CustomerId
                        };

                        _repository.AddEntity(reading);
                    }

                    if (await _repository.SaveAllAsync())
                    {
                        _logger.LogInformation($"Stored {request.Readings.Count} New readings...");
                        result.Success = ReadingStatus.Success;
                    }
                }
                catch (RpcException)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    _logger.LogError($"Exception thrown during saving of readings: {ex}");
                    throw new RpcException(Status.DefaultCancelled, "Exception thrown during process.");
                }
            }

            return result;
        }
    }
}
