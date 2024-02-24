namespace OrleansWorkbench.Etcd;

/// <summary>
/// Represents an exception which occurred in the Redis clustering.
/// </summary>
[Serializable]
public class EtcdClusteringException : Exception
{
    /// <inheritdoc/>
    public EtcdClusteringException() : base()
    {
    }

    /// <inheritdoc/>
    public EtcdClusteringException(string message) : base(message)
    {
    }

    /// <inheritdoc/>
    public EtcdClusteringException(string message, Exception innerException) : base(message, innerException)
    {
    }
}