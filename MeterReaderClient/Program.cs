using MeterReaderClient;

IHost host = Host.CreateDefaultBuilder(args)
    .ConfigureServices(services =>
    {
        services.AddHostedService<Worker>();
        services.AddTransient<ReadingFactory>();
    })
    .Build();

await host.RunAsync();
