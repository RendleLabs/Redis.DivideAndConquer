using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace RendleLabs.Redis.DivideAndConquer
{
    public class DivideAndConquerSubscriber : IDivideAndConquerSubscriber
    {
        private readonly ConcurrentDictionary<RedisKey, IList<Func<RedisKey, RedisValue, Task>>> _subscriptions = new ConcurrentDictionary<RedisKey, IList<Func<RedisKey, RedisValue, Task>>>();
        private readonly ConcurrentDictionary<RedisKey, bool> _running = new ConcurrentDictionary<RedisKey, bool>();
        private readonly ConnectionMultiplexer _redis;
        private readonly RedisChannel _pubSubChannel;
        private readonly object _sync = new object();
        private IDatabase _db;
        private ISubscriber _subscriber;
        public DivideAndConquerSubscriber(ConnectionMultiplexer redis, RedisChannel pubSubChannel)
        {
            _redis = redis;
            _pubSubChannel = pubSubChannel;
        }

        public void Subscribe(RedisKey key, Func<RedisKey, RedisValue, Task> action)
        {
            if (_subscriber == null) Open();
            var list = _subscriptions.GetOrAdd(key, _ => new List<Func<RedisKey, RedisValue, Task>>());
            list.Add(action);
        }

        public void Unsubscribe(RedisKey key, Func<RedisKey, RedisValue, Task> action = null)
        {
            if (_subscriber == null) return;

            IList<Func<RedisKey, RedisValue, Task>> subscriptions;
            if (action == null)
            {
                _subscriptions.TryRemove(key, out subscriptions);
            }
            else if (_subscriptions.TryGetValue(key, out subscriptions))
            {
                subscriptions.Remove(action);
                if (subscriptions.Count == 0)
                {
                    _subscriptions.TryRemove(key, out subscriptions);
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
                    _subscriber.Subscribe(_pubSubChannel, PubSubMessageHandler);
                }
            }
        }

        private void Close()
        {
            lock (_sync)
            {
                if (_subscriptions.Count == 0 && _subscriber != null)
                {
                    _subscriber.UnsubscribeAll();
                    _subscriber = null;
                    _db = null;
                }
            }
        }

        private void PubSubMessageHandler(RedisChannel channel, RedisValue value)
        {
            if (value.IsNullOrEmpty) return;
            var key = (string)value;

            IList<Func<RedisKey, RedisValue, Task>> subscriptions;
            if (_subscriptions.TryGetValue(key, out subscriptions))
            {
                if (!_running.TryAdd(key, true))
                {
                    return;
                }

                try
                {
                    ProcessQueue(key, subscriptions);
                }
                catch { }
            }
        }

        private async void ProcessQueue(RedisKey key, IList<Func<RedisKey, RedisValue, Task>> actions)
        {
            try
            {
                RedisValue value;
                while ((value = await _db.ListLeftPopAsync(key)).HasValue)
                {
                    for (int i = 0; i < actions.Count; i++)
                    {
                        FireAndForget(actions[i], key, value);
                    }
                }
            }
            finally
            {
                bool _;
                _running.TryRemove(key, out _);
            }
        }

        private static async void FireAndForget(Func<RedisKey, RedisValue, Task> action, RedisKey key, RedisValue value)
        {
            try
            {
                await action(key, value);
            }
            catch { }
        }
    }
}