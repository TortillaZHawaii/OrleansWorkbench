using Grpc.Core;

namespace OrleansWorkbench.Etcd;

class EtcdClusteringOptions
{
    public required string ConnectionString { get; init; }
    public Metadata? GrpcHeaders { get; init; }
}
