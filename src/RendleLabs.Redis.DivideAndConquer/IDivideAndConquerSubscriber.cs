using System;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace RendleLabs.Redis.DivideAndConquer
{
    public interface IDivideAndConquerSubscriber : IDisposable
    {
        void Subscribe(Func<RedisValue, RedisValue, Task> action);

        void Unsubscribe(Func<RedisValue, RedisValue, Task> action = null);

    }
}