using System.Threading.Tasks;
using StackExchange.Redis;

namespace RendleLabs.Redis.DivideAndConquer
{
    public class DivideAndConquerPublisher : IDivideAndConquerPublisher
    {
        private readonly ConnectionMultiplexer _redis;
        private readonly RedisChannel _pubSubChannel;
        private readonly object _sync = new object();
        private IDatabase _db;
        private ISubscriber _subscriber;

        public DivideAndConquerPublisher(ConnectionMultiplexer redis, RedisChannel pubSubChannel)
        {
            _redis = redis;
            _pubSubChannel = pubSubChannel;
        }

        public async Task<long> PublishAsync(RedisKey key, RedisValue value)
        {
            if (_subscriber == null) Open();
            var count = await _db.ListRightPushAsync(key, value);
            await _subscriber.PublishAsync(_pubSubChannel, key.ToString());
            return count;
        }

        public async Task<long> PublishAsync(RedisKey key, RedisValue[] values)
        {
            if (_subscriber == null) Open();
            var count = await _db.ListRightPushAsync(key, values);
            await _subscriber.PublishAsync(_pubSubChannel, key.ToString());
            return count;
        }
        
        private void Open()
        {
            lock (_sync)
            {
                if (_subscriber == null)
                {
                    _subscriber = _redis.GetSubscriber();
                    _db = _redis.GetDatabase();
                }
            }
        }
    }
}
