using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

IHostBuilder builder = Host.CreateDefaultBuilder(args)
    .UseOrleans(silo =>
    {
        silo.UseEtcdClustering("http://localhost:2379")
            .ConfigureLogging(logging => logging.AddConsole());
    })
    .UseConsoleLifetime();

using IHost host = builder.Build();

await host.RunAsync();
