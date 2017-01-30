using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using StackExchange.Redis;

namespace RendleLabs.Redis.DivideAndConquer
{
    public static class DivideAndConquerExtensions
    {
        private static readonly ConditionalWeakTable<ConnectionMultiplexer, ConcurrentDictionary<RedisChannel, IDivideAndConquerSubscriber>> Cache
            = new ConditionalWeakTable<ConnectionMultiplexer, ConcurrentDictionary<RedisChannel, IDivideAndConquerSubscriber>>();

        public static IDivideAndConquerSubscriber GetDivideAndConquerSubscriber(this ConnectionMultiplexer redis, RedisChannel channel)
        {
            var dict = Cache.GetValue(redis, _ => new ConcurrentDictionary<RedisChannel, IDivideAndConquerSubscriber>());
            return dict.GetOrAdd(channel, ch => new DivideAndConquerSubscriber(redis, ch));
        }
    }
}