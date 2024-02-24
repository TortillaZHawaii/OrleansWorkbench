using Orleans.Configuration;
using Orleans.Messaging;
using Orleans.Runtime;

namespace OrleansWorkbench.Etcd;

public class EtcdGatewayListProvider(EtcdMembershipTable etcdMembershipTable, GatewayOptions gatewayOptions)
    : IGatewayListProvider
{
    public Task InitializeGatewayListProvider()
    {
        return etcdMembershipTable.InitializeMembershipTable(true);
    }

    public async Task<IList<Uri>> GetGateways()
    {
        if (!etcdMembershipTable.IsInitialized)
        {
            await etcdMembershipTable.InitializeMembershipTable(true);
        }
        
        var all = await etcdMembershipTable.ReadAll();
        var result = all.Members
            .Where(x => x.Item1.Status == SiloStatus.Active && x.Item1.ProxyPort != 0)
            .Select(x =>
            {
                x.Item1.SiloAddress.Endpoint.Port = x.Item1.ProxyPort;
                return x.Item1.SiloAddress.ToGatewayUri();
            }).ToList();
        
        return result;
    }

    public TimeSpan MaxStaleness => gatewayOptions.GatewayListRefreshPeriod;
    public bool IsUpdatable => true;
}