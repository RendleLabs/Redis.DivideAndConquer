using System;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json;
using RendleLabs.Redis.DivideAndConquer;
using StackExchange.Redis;

namespace Consumer
{
    public class Program
    {
        private static string _group;
        public static void Main(string[] args)
        {
            _group = args.FirstOrDefault();
            if (_group != null)
            {
                Console.WriteLine($"Group: {_group}");
            }
            var subscriber = Consume();
            Console.WriteLine("Press any key to exit.");
            Console.ReadKey();
        }

        private static IDivideAndConquerSubscriber Consume()
        {
            var completedTask = Task.FromResult<object>(null);
            var redis = ConnectionMultiplexer.Connect("127.0.0.1:6379");
            var subscriber = string.IsNullOrWhiteSpace(_group)
                ? redis.GetDivideAndConquerSubscriber("test")
                : redis.GetDivideAndConquerSubscriber("test", _group);

            subscriber.Subscribe((metadataJson, value) => {
                var metadata = JsonConvert.DeserializeObject<Metadata>(metadataJson);
                Console.WriteLine($"[{_group}] Got: {metadata.JobId} - {value}");
                return completedTask;
            });
            return subscriber;
        }
    }
    class Metadata
    {
        public string JobId { get; set; }
    }
}
