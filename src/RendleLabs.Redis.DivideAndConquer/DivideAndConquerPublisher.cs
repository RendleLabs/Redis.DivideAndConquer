using System;
using System.Threading.Tasks;
using StackExchange.Redis;
using Google.Protobuf;

namespace RendleLabs.Redis.DivideAndConquer
{
    public class DivideAndConquerPublisher : IDivideAndConquerPublisher
    {
        private readonly ConnectionMultiplexer _redis;
        private readonly RedisChannel _pubSubChannel;
        private readonly object _sync = new object();
        private readonly RedisKey _pubSubGroupsHash;
        private IDatabase _db;
        private ISubscriber _subscriber;

        public DivideAndConquerPublisher(ConnectionMultiplexer redis, RedisChannel pubSubChannel)
        {
            _redis = redis;
            _pubSubChannel = pubSubChannel;
            _pubSubGroupsHash = $"RLRDAC:{_pubSubChannel}:GROUPS";
        }

        public Task<long> PublishAsync(RedisValue metadata, RedisValue value)
        {
            return PublishAsync(metadata, new[] {value});
        }

        public async Task<long> PublishAsync(RedisValue metadata, RedisValue[] values)
        {
            if (_subscriber == null) Open();

            var listKeyBase = $"{_pubSubChannel}.{Guid.NewGuid():N}";
            var x = Google.Protobuf.ByteString.CopyFromUtf8(listKeyBase);
            var msg = new PubSubMessage
            {
                ListKey = ByteString.CopyFromUtf8(listKeyBase),
                Metadata = ByteString.CopyFrom(metadata)
            };

            long count = 0;

            foreach (var pair in _db.HashScan(_pubSubGroupsHash))
            {
                int subscribers;
                if (pair.Value.TryParse(out subscribers) && subscribers > 0)
                {
                    var listKey = $"{listKeyBase}:{pair.Name}";
                    count = await _db.ListRightPushAsync(listKey, values);
                }
            }

            await _subscriber.PublishAsync(_pubSubChannel, msg.ToByteArray());
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
