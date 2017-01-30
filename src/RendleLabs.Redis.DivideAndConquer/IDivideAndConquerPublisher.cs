using System.Threading.Tasks;
using StackExchange.Redis;

namespace RendleLabs.Redis.DivideAndConquer
{
    public interface IDivideAndConquerPublisher
    {
        Task<long> PublishAsync(RedisKey key, RedisValue value);
        Task<long> PublishAsync(RedisKey key, RedisValue[] values);
    }
}