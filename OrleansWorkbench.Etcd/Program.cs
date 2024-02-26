using System.Globalization;
using dotnet_etcd;
using Google.Protobuf;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using Orleans.Configuration;
using Orleans.Runtime;
using JsonSerializer = System.Text.Json.JsonSerializer;


var etcdClient = new EtcdClient("http://localhost:2379");

var tableVersion = new TableVersion(1, "1");
var previousTableVersion = new TableVersion(0, "0");
var tableVersionKey = $"Orleans/default/Members/default/Version";
var tableVersionByteKey = ByteString.CopyFromUtf8(tableVersionKey);
var rowKey = "Orleans/default/Members/default/1";
var rowByteKey = ByteString.CopyFromUtf8(rowKey);

var txn = new Etcdserverpb.TxnRequest
{
    Compare =
    {
        new Etcdserverpb.Compare
        {
            Key = tableVersionByteKey,
            Result = Etcdserverpb.Compare.Types.CompareResult.Equal,
            Target = Etcdserverpb.Compare.Types.CompareTarget.Value,
            Value = SerializeVersion(tableVersion),
        },
    },
    Success =
    {
        new Etcdserverpb.RequestOp
        {
            RequestPut = new Etcdserverpb.PutRequest()
            {
                Key = rowByteKey,
                Value = ByteString.CopyFromUtf8("ABCDEFGH"),
            },
        },
    },
    Failure =
    {
        new Etcdserverpb.RequestOp
        {
            RequestRange = new Etcdserverpb.RangeRequest
            {
                Key = tableVersionByteKey,
            },
        },
    },
};

var txnResponse = await etcdClient.TransactionAsync(txn);

Console.WriteLine(txnResponse.Responses[1].ResponseRange.Kvs[0].Value.ToStringUtf8());

static ByteString SerializeVersion(TableVersion version) =>
    ByteString.CopyFromUtf8(version.Version.ToString(CultureInfo.InvariantCulture));