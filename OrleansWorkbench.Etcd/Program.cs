using System.Globalization;
using dotnet_etcd;
using Google.Protobuf;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using Orleans.Configuration;
using Orleans.Runtime;
using JsonSerializer = System.Text.Json.JsonSerializer;


var etcdClient = new EtcdClient("http://localhost:2379");

await etcdClient.PutAsync("foo/bar", "baz");
var response = await etcdClient.GetValAsync("foo/bar");

Console.WriteLine(response);
