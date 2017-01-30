using System;
using System.Threading.Tasks;
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
            var redis = ConnectionMultiplexer.Connect("127.0.0.1:6379");
            var subscriber = redis.GetDivideAndConquerSubscriber("test");
            subscriber.Subscribe("work", async (key, value) => {
                if (!string.IsNullOrWhiteSpace(value))
                {
                    Console.WriteLine();
                    Console.WriteLine($"Got: {value}");
                    await Task.Delay(10);
                }
                else
                {
                    Console.Write(".");
                    await Task.Delay(100);
                }
            });
            return subscriber;
        }
    }
}
