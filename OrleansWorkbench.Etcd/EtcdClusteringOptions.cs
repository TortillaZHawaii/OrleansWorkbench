using Grpc.Core;

namespace OrleansWorkbench.Etcd;

public class EtcdClusteringOptions
{
    public required string ConnectionString { get; set; }
    public Metadata? GrpcHeaders { get; set; }
}
