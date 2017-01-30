using System;
using System.Linq;
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

            var guids = Enumerable.Repeat(0, 1000).Select(_ => (RedisValue)Guid.NewGuid().ToString()).ToArray();
            pub.PublishAsync("work", guids).Wait();
        }
    }
}
