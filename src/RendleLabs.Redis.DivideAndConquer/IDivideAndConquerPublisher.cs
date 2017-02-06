using System.Threading.Tasks;
using StackExchange.Redis;

namespace RendleLabs.Redis.DivideAndConquer
{
    public interface IDivideAndConquerPublisher
    {
        Task<long> PublishAsync(RedisValue metadata, RedisValue value);
        Task<long> PublishAsync(RedisValue metadata, RedisValue[] values);
    }
}