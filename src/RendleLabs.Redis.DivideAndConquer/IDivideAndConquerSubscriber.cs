using System;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace RendleLabs.Redis.DivideAndConquer
{
    public interface IDivideAndConquerSubscriber
    {
        void Subscribe(RedisKey key, Func<RedisKey, RedisValue, Task> action);

        void Unsubscribe(RedisKey key, Func<RedisKey, RedisValue, Task> action = null);

    }
}