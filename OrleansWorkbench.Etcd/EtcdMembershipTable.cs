using System.Globalization;
using System.Runtime.InteropServices;
using System.Text.Json;
using dotnet_etcd;
using Google.Protobuf;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Runtime;

namespace OrleansWorkbench.Etcd;

public class EtcdMembershipTable : IMembershipTable, IDisposable
{
    private const string TableVersionKeySuffix ="Version";
    private const string OrleansPrefix = "Orleans";
    private readonly string _tableVersionKey;
    private readonly ByteString _tableVersionByteKey;
    private static readonly TableVersion DefaultTableVersion = new(0, "0");
    private readonly EtcdClusteringOptions _etcdOptions;
    private readonly ClusterOptions _clusterOptions;
    private readonly string _clusterKey;
    private readonly EtcdClient _etcdClient;
    private readonly ILogger<EtcdMembershipTable> _logger;
    
    public EtcdMembershipTable(IOptions<EtcdClusteringOptions> etcdOptions, IOptions<ClusterOptions> clusterOptions,
        ILoggerFactory loggerFactory)
    {
        _logger = loggerFactory.CreateLogger<EtcdMembershipTable>();
        _etcdOptions = etcdOptions.Value;
        _clusterOptions = clusterOptions.Value;
        _clusterKey = $"{OrleansPrefix}/{_clusterOptions.ServiceId}/Members/{_clusterOptions.ClusterId}";
        _tableVersionKey = $"{_clusterKey}/{TableVersionKeySuffix}";
        _tableVersionByteKey = ByteString.CopyFromUtf8(TableVersionKeySuffix);
        _etcdClient = new EtcdClient(_etcdOptions.ConnectionString);
    }

    public bool IsInitialized { get; private set; }
    
    public async Task InitializeMembershipTable(bool tryInitTableVersion)
    {
        if (tryInitTableVersion)
        {
            var txn = new Etcdserverpb.TxnRequest
            {
                Compare =
                {
                    new Etcdserverpb.Compare
                    {
                        Key = _tableVersionByteKey,
                        Result = Etcdserverpb.Compare.Types.CompareResult.Equal,
                        Target = Etcdserverpb.Compare.Types.CompareTarget.Create,
                        CreateRevision = 0,
                    },
                },
                Success =
                {
                    new Etcdserverpb.RequestOp
                    {
                        RequestPut = new Etcdserverpb.PutRequest
                        {
                            Key = _tableVersionByteKey,
                            Value = SerializeVersion(DefaultTableVersion),
                        },
                    },
                },
            };
            var response = await _etcdClient.TransactionAsync(txn, _etcdOptions.GrpcHeaders);
            if (response.Succeeded)
            {
                _logger.LogInformation("Initialized table version for cluster {ClusterId}", _clusterOptions.ClusterId);
            }
        }
        
        IsInitialized = true;
    }

    public async Task DeleteMembershipTableEntries(string clusterId)
    {
        _logger.LogInformation("Deleting membership table entries for cluster {ClusterId}", clusterId);
        await _etcdClient.DeleteRangeAsync(clusterId, _etcdOptions.GrpcHeaders);
    }
    
    public async Task CleanupDefunctSiloEntries(DateTimeOffset beforeDate)
    {
        _logger.LogInformation("Cleaning up defunct silo entries for cluster {ClusterId}", _clusterOptions.ClusterId);
        var entries = await ReadAll();
        foreach (var (entry, _) in entries.Members)
        {
            if (entry.Status != SiloStatus.Active 
                && new DateTime(Math.Max(entry.IAmAliveTime.Ticks, entry.StartTime.Ticks), DateTimeKind.Utc) < beforeDate)
            {
                // best effort
                await _etcdClient.DeleteRangeAsync(entry.SiloAddress.ToString(), _etcdOptions.GrpcHeaders);
            }
        }
    }

    public async Task<MembershipTableData> ReadRow(SiloAddress key)
    {
        _logger.LogInformation("Reading row for key {Key}", key);
        var txn = new Etcdserverpb.TxnRequest
        {
            Success =
            {
                new Etcdserverpb.RequestOp
                {
                    RequestRange = new Etcdserverpb.RangeRequest
                    {
                        Key = _tableVersionByteKey,
                    }
                },
                new Etcdserverpb.RequestOp
                {
                    RequestRange = new Etcdserverpb.RangeRequest
                    {
                        Key = ByteString.CopyFromUtf8($"{_clusterKey}/{key}")
                    }
                },
            },
        };

        var response = await _etcdClient.TransactionAsync(txn, _etcdOptions.GrpcHeaders);

        if (!response.Succeeded)
        {
            throw new EtcdClusteringException($"Unexpected transaction failure while reading key {key}");
        }
        
        var tableVersion = DeserializeVersion(response.Responses[0].ResponseRange.Kvs[0].Value);
        var entry = response.Responses[1].ResponseRange.Kvs[0].Value;
        
        if (entry.Length > 0)
        {
            return new MembershipTableData(Tuple.Create(Deserialize(entry.Span), tableVersion.VersionEtag), tableVersion);
        } 
        else
        {
            return new MembershipTableData(tableVersion);
        }
    }

    public async Task<MembershipTableData> ReadAll()
    {
        _logger.LogInformation("Reading all rows for cluster {ClusterId}", _clusterOptions.ClusterId);
        var all = await _etcdClient.GetRangeAsync(_clusterKey, _etcdOptions.GrpcHeaders);
        
        var tableVersionRow = all.Kvs.SingleOrDefault(h => _tableVersionByteKey.Equals(h.Key))?.Value;
        var tableVersion = tableVersionRow == null || tableVersionRow.IsEmpty
            ? DefaultTableVersion
            : DeserializeVersion(tableVersionRow);
        
        var data = all.Kvs.Where(h => !_tableVersionByteKey.Equals(h.Key) && h.Value.Length > 0)
            .Select(x => Tuple.Create(Deserialize(x.Value.Span), tableVersion.VersionEtag))
            .ToList();
        
        return new MembershipTableData(data, tableVersion);
    }

    public async Task<bool> InsertRow(MembershipEntry entry, TableVersion tableVersion)
    {
        _logger.LogInformation("Inserting row for key {Key}", entry.SiloAddress);
        return await UpsertRowInternal(entry, tableVersion, updateTableVersion: true, allowInsertOnly: true) 
               == UpsertResult.Success;
    }

    public async Task<bool> UpdateRow(MembershipEntry entry, string etag, TableVersion tableVersion)
    {
        _logger.LogInformation("Updating row for key {Key}", entry.SiloAddress);
        return await UpsertRowInternal(entry, tableVersion, updateTableVersion: true, allowInsertOnly: false) 
               == UpsertResult.Success;
    }
    
    private async Task<UpsertResult> UpsertRowInternal(MembershipEntry entry, TableVersion tableVersion, 
        bool updateTableVersion, bool allowInsertOnly)
    {
        var txn = new Etcdserverpb.TxnRequest();
        var rowKey = $"{_clusterKey}/{entry.SiloAddress}";
        var rowByteKey = ByteString.CopyFromUtf8(rowKey);
        
        _logger.LogInformation("Upserting row for key {Key}", rowKey);
        
        if (updateTableVersion)
        {
            _logger.LogInformation("Setting table version {TableVersion} for cluster {ClusterId}", tableVersion, _clusterOptions.ClusterId);
            txn.Success.Add(new Etcdserverpb.RequestOp
            {
                RequestPut =
                {
                    Key = _tableVersionByteKey,
                    Value = SerializeVersion(tableVersion),
                },
            });
            _logger.LogInformation("Set table version {TableVersion} for cluster {ClusterId}", tableVersion, _clusterOptions.ClusterId);
        }
        
        txn.Compare.Add(new Etcdserverpb.Compare
        {
            Key = _tableVersionByteKey,
            Result = Etcdserverpb.Compare.Types.CompareResult.Equal,
            Target = Etcdserverpb.Compare.Types.CompareTarget.Value,
            Value = SerializeVersion(Predecessor(tableVersion)),
        });
        
        if (allowInsertOnly)
        {
            txn.Compare.Add(new Etcdserverpb.Compare
            {
                Key = rowByteKey,
                Result = Etcdserverpb.Compare.Types.CompareResult.Equal,
                Target = Etcdserverpb.Compare.Types.CompareTarget.Create,
                CreateRevision = 0,
            });
        }
        
        txn.Success.Add(new Etcdserverpb.RequestOp
        {
            RequestPut =
            {
                Key = rowByteKey,
                Value = ByteString.CopyFrom(Serialize(entry)),
            },
        });
        
        txn.Failure.Add(new Etcdserverpb.RequestOp
        {
            RequestRange = new Etcdserverpb.RangeRequest
            {
                Key = _tableVersionByteKey,
            },
        });
        
        var response = await _etcdClient.TransactionAsync(txn, _etcdOptions.GrpcHeaders);
        
        if (response.Succeeded)
        {
            _logger.LogInformation("Upserted row for key {Key}", rowKey);
            return UpsertResult.Success;
        }
        
        var tableVersionRow = DeserializeVersion(response.Responses[0].ResponseRange.Kvs[0].Value);
        if (tableVersionRow != tableVersion)
        {
            _logger.LogWarning("Failed to upsert row for key {Key} due to conflict, expected version to be {ExpectedVersion}, but found {FoundVersion}", rowKey, tableVersion, tableVersionRow);
            return UpsertResult.Conflict;
        }

        _logger.LogWarning("Failed to upsert row for key {Key}", rowKey);
        return UpsertResult.Failure;
    }

    public async Task UpdateIAmAlive(MembershipEntry entry)
    {
        var key = entry.SiloAddress.ToString();
        var txn = new Etcdserverpb.TxnRequest
        {
            Success =
            {
                new Etcdserverpb.RequestOp
                {
                    RequestRange = new Etcdserverpb.RangeRequest
                    {
                        Key = _tableVersionByteKey,
                    },
                },
                new Etcdserverpb.RequestOp
                {
                    RequestRange = new Etcdserverpb.RangeRequest
                    {
                        Key = ByteString.CopyFromUtf8(key),
                    },
                },
            }
        };
        
        var response = await _etcdClient.TransactionAsync(txn, _etcdOptions.GrpcHeaders);
        
        if (!response.Succeeded)
        {
            _logger.LogWarning("Unexpected transaction failure while reading key {Key}", key);
            throw new EtcdClusteringException($"Unexpected transaction failure while reading key {key}");
        }
        
        var tableVersion = DeserializeVersion(response.Responses[0].ResponseRange.Kvs[0].Value);
        var entryRow = response.Responses[1].ResponseRange.Kvs[0].Value;
        
        if (entryRow == null || entryRow.IsEmpty)
        {
            _logger.LogWarning("Could not find a value for the key {Key}", key);
            throw new EtcdClusteringException($"Could not find a value for the key {key}");
        }
        
        var existingEntry = Deserialize(entryRow.Span);
        
        // Update only the IAmAliveTime property.
        existingEntry.IAmAliveTime = entry.IAmAliveTime;
        
        var result = await UpsertRowInternal(existingEntry, tableVersion, updateTableVersion: false, allowInsertOnly: false);
        
        if (result == UpsertResult.Conflict)
        {
            throw new EtcdClusteringException($"Failed to update IAmAlive value for key {key} due to conflict");
        }
        else if (result != UpsertResult.Success)
        {
            throw new EtcdClusteringException($"Failed to update IAmAlive value for key {key}");
        }
    }

    public void Dispose()
    {
        _etcdClient.Dispose();
    }

    private static ByteString SerializeVersion(TableVersion version) =>
        ByteString.CopyFromUtf8(version.VersionEtag);

    private static TableVersion DeserializeVersion(string versionString)
    {
        if (string.IsNullOrEmpty(versionString))
        {
            return DefaultTableVersion;
        }

        return new TableVersion(int.Parse(versionString, CultureInfo.InvariantCulture), versionString);
    }

    private static TableVersion DeserializeVersion(ByteString versionString) =>
        DeserializeVersion(versionString.ToStringUtf8());

    private static TableVersion Predecessor(TableVersion tableVersion) => new TableVersion(tableVersion.Version - 1,
        (tableVersion.Version - 1).ToString(CultureInfo.InvariantCulture));
    
    private ReadOnlySpan<byte> Serialize(MembershipEntry entry)
    {
        return JsonSerializer.SerializeToUtf8Bytes(entry);
    }
    
    private MembershipEntry Deserialize(ReadOnlySpan<byte> entry)
    {
        return JsonSerializer.Deserialize<MembershipEntry>(entry)!;
    }
    
    enum UpsertResult
    {
        Success,
        Failure,
        Conflict,
    }
}