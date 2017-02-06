using StackExchange.Redis;

namespace RendleLabs.Redis.DivideAndConquer
{
    public static class DivideAndConquerExtensions
    {
        public static IDivideAndConquerSubscriber GetDivideAndConquerSubscriber(this ConnectionMultiplexer redis, RedisChannel channel)
        {
            return new DivideAndConquerSubscriber(redis, channel);
        }
    }
}