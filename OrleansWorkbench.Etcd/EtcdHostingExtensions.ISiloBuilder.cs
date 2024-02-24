using Microsoft.Extensions.DependencyInjection;
using Orleans.Messaging;
using OrleansWorkbench.Etcd;

// ReSharper disable once CheckNamespace
namespace Microsoft.Extensions.Hosting;

public static class EtcdClusteringISiloBuilderExtensions
{
    public static ISiloBuilder UseEtcdClustering(this ISiloBuilder builder, Action<EtcdClusteringOptions>? configureOptions)
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
    
    public static ISiloBuilder UseEtcdClustering(this ISiloBuilder builder, string connectionString)
    {
        return builder.ConfigureServices(services => services
            .Configure<EtcdClusteringOptions>(options =>
            {
                options.ConnectionString = connectionString;
            })
            .AddEtcdClustering()
            .AddSingleton<IGatewayListProvider, EtcdGatewayListProvider>());
    }
    
    internal static IServiceCollection AddEtcdClustering(this IServiceCollection services)
    {
        services.AddSingleton<EtcdMembershipTable>();
        services.AddSingleton<IMembershipTable>(sp => sp.GetRequiredService<EtcdMembershipTable>());
        return services;
    }
}