using System;
using System.Text;
using System.Threading;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace EdfWorker
{
    class Program
    {
        public static void Main()
        {
            var factory = new ConnectionFactory
            {
                Uri = "amqp://ikjclzkh:6BmsMH2hYq8PQUBP-ELD-WHOgAJ931Rz@zebra.rmq.cloudamqp.com/ikjclzkh",
                RequestedHeartbeat = 60
            };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                var q = "edf_asset"; 

                channel.QueueDeclare(queue: q,
                    durable: true,
                    exclusive: false,
                    autoDelete: false,
                    arguments: null);

                channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);

                Console.WriteLine(" [*] Waiting for messages.");

                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, ea) =>
                {
                    var body = ea.Body;
                    var message = Encoding.UTF8.GetString(body);
                    Console.WriteLine(" [x] Received {0}", message);

                    int dots = message.Split('.').Length - 1;
                    Thread.Sleep(dots * 1000);

                    Console.WriteLine(" [x] Done");

                    channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                };
                channel.BasicConsume(queue: q,
                    noAck: false,
                    consumer: consumer);

                Console.WriteLine(" Press [enter] to exit.");
                Console.ReadLine();
            }
        }
    }
}