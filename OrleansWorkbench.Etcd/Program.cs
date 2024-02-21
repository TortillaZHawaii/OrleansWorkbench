using dotnet_etcd;

var etcdClient = new EtcdClient("http://localhost:2379");

await etcdClient.PutAsync("foo/bar", "baz");
var response = await etcdClient.GetValAsync("foo/bar");

Console.WriteLine(response);
