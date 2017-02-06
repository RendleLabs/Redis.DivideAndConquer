using System;
using System.Threading.Tasks;
using Newtonsoft.Json;
using RendleLabs.Redis.DivideAndConquer;
using StackExchange.Redis;

namespace Consumer
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var subscriber = Consume();
            Console.WriteLine("Press any key to exit.");
            Console.ReadKey();
        }

        private static IDivideAndConquerSubscriber Consume()
        {
            var completedTask = Task.FromResult<object>(null);
            var redis = ConnectionMultiplexer.Connect("127.0.0.1:6379");
            var subscriber = redis.GetDivideAndConquerSubscriber("test");
            subscriber.Subscribe((metadataJson, value) => {
                var metadata = JsonConvert.DeserializeObject<Metadata>(metadataJson);
                Console.WriteLine($"Got: {metadata.JobId} - {value}");
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
