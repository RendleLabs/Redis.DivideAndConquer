using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using StackExchange.Redis;

namespace RendleLabs.Redis.DivideAndConquer
{
    public static class DivideAndConquerPublisherEx
    {
        private static readonly ConditionalWeakTable<ConnectionMultiplexer, ConcurrentDictionary<RedisChannel, IDivideAndConquerPublisher>> Cache
            = new ConditionalWeakTable<ConnectionMultiplexer, ConcurrentDictionary<RedisChannel, IDivideAndConquerPublisher>>();

        public static IDivideAndConquerPublisher GetDivideAndConquerPublisher(this ConnectionMultiplexer redis, RedisChannel channel)
        {
            var dict = Cache.GetValue(redis, _ => new ConcurrentDictionary<RedisChannel, IDivideAndConquerPublisher>());
            return dict.GetOrAdd(channel, ch => new DivideAndConquerPublisher(redis, ch));
        }
    }
}