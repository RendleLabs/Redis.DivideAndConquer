using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace RendleLabs.Redis.DivideAndConquer
{
    public class DivideAndConquerSubscriber : IDivideAndConquerSubscriber, IDisposable
    {
        private readonly List<Func<RedisValue, RedisValue, Task>> _subscriptions = new List<Func<RedisValue, RedisValue, Task>>();
        private readonly ConcurrentDictionary<RedisKey, bool> _running = new ConcurrentDictionary<RedisKey, bool>();
        private readonly object _subscriptionsSync = new object();
        private readonly ConnectionMultiplexer _redis;
        private readonly RedisChannel _pubSubChannel;
        private readonly RedisKey _pubSubGroupsHash;
        private readonly RedisValue _groupName;
        private readonly RedisKey _groupSuffix;
        private readonly object _sync = new object();
        private IDatabase _db;
        private ISubscriber _subscriber;

        public DivideAndConquerSubscriber(ConnectionMultiplexer redis, RedisChannel pubSubChannel)
            : this(redis, pubSubChannel, "RLRDAC_PUBSUB_NOGROUP") { }

        public DivideAndConquerSubscriber(ConnectionMultiplexer redis, RedisChannel pubSubChannel, RedisValue groupName)
        {
            _redis = redis;
            _pubSubChannel = pubSubChannel;
            _pubSubGroupsHash = $"RLRDAC:{_pubSubChannel}:GROUPS";
            _groupName = groupName;
            _groupSuffix = $":{groupName}";
        }

        public void Subscribe(Func<RedisValue, RedisValue, Task> action)
        {
            if (_subscriber == null) Open();
            lock (_subscriptionsSync)
            {
                _subscriptions.Add(action);
            }
        }

        public void Unsubscribe(Func<RedisValue, RedisValue, Task> action = null)
        {
            if (_subscriber == null || _subscriptions.Count == 0) return;

            lock (_subscriptionsSync)
            {
                if (action == null)
                {
                    _subscriptions.Clear();
                }
                else
                {
                    _subscriptions.Remove(action);
                }
            }

            if (_subscriptions.Count == 0)
            {
                Close();
            }
        }

        private void Open()
        {
            lock (_sync)
            {
                if (_subscriber == null)
                {
                    _subscriber = _redis.GetSubscriber();
                    _db = _redis.GetDatabase();
                    _db.HashIncrement(_pubSubGroupsHash, _groupName);
                    _subscriber.Subscribe(_pubSubChannel, PubSubMessageHandler);
                }
            }
        }

        private void Close()
        {
            if (_subscriber != null)
            {
                lock (_sync)
                {
                    if (_subscriber != null)
                    {
                        _db.HashDecrement(_pubSubGroupsHash, _groupName);
                        _subscriber.Unsubscribe(_pubSubChannel, PubSubMessageHandler);
                        _subscriber = null;
                        _db = null;
                    }
                }
            }
        }

        private void PubSubMessageHandler(RedisChannel channel, RedisValue value)
        {
            if (value.IsNullOrEmpty || _subscriptions.Count == 0) return;
            var msg = PubSubMessage.Parser.ParseFrom(value);

            if (!_running.TryAdd(msg.ListKey.ToStringUtf8(), true)) return;

            try
            {
                ProcessQueue(msg);
            }
            catch { } // Not awaiting this task
        }

        private async void ProcessQueue(PubSubMessage message)
        {
            var listKey = ((RedisKey)message.ListKey.ToByteArray()).Append(_groupSuffix);
            var metadata = message.Metadata.ToByteArray();
            var actions = _subscriptions.ToArray();
            try
            {
                RedisValue value;
                while ((value = await _db.ListLeftPopAsync(listKey)).HasValue)
                {
                    for (int i = 0; i < actions.Length; i++)
                    {
                        FireAndForget(actions[i], metadata, value);
                    }
                    await Task.Delay(1);
                }
            }
            finally
            {
                bool _;
                _running.TryRemove(message.ListKey.ToStringUtf8(), out _);
            }
        }

        private static async void FireAndForget(Func<RedisValue, RedisValue, Task> action, RedisValue metadata, RedisValue value)
        {
            try
            {
                await action(metadata, value);
            }
            catch { }
        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    // TODO: dispose managed state (managed objects).
                }

                Close();

                disposedValue = true;
            }
        }

        ~DivideAndConquerSubscriber()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(false);
        }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
            GC.SuppressFinalize(this);
        }
        #endregion
    }
}