using System;
using System.Linq;
using Newtonsoft.Json;
using RendleLabs.Redis.DivideAndConquer;
using StackExchange.Redis;

namespace Producer
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var redis = ConnectionMultiplexer.Connect("127.0.0.1:6379");
            var pub = redis.GetDivideAndConquerPublisher("test");

            var guids = Enumerable.Repeat(0, 20).Select(_ => (RedisValue)Guid.NewGuid().ToString()).ToArray();
            var metadata = JsonConvert.SerializeObject(new Metadata { JobId = "42" });
            pub.PublishAsync(metadata, guids).Wait();
        }
    }

    class Metadata
    {
        public string JobId { get; set; }
    }
}
