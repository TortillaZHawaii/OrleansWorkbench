using Microsoft.Extensions.DependencyInjection;
using Orleans.Messaging;
using OrleansWorkbench.Etcd;

// ReSharper disable once CheckNamespace
namespace Microsoft.Extensions.Hosting;

public static class EtcdClusteringIClientBuilderExtensions
{
    public static IClientBuilder UseEtcdClustering(this IClientBuilder builder, Action<EtcdClusteringOptions>? configureOptions)
    {
        return builder.ConfigureServices(services =>
        {
            if (configureOptions != null)
            {
                services.Configure(configureOptions);
            }

            services
                .AddEtcdClustering()
                .AddSingleton<IGatewayListProvider, EtcdGatewayListProvider>();
        });
    }

    public static IClientBuilder UseEtcdClustering(this IClientBuilder builder, string connectionString)
    {
        return builder.ConfigureServices(services => services
            .Configure<EtcdClusteringOptions>(options =>
            {
                options.ConnectionString = connectionString;
            })
            .AddEtcdClustering()
            .AddSingleton<IGatewayListProvider, EtcdGatewayListProvider>());
    }
}