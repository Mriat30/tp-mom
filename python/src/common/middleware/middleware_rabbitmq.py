import pika
import random
import string
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=queue_name)
        self.queue_name = queue_name

    def start_consuming(self, on_message_callback):
        self.channel.basic_consume(queue=self.queue_name, on_message_callback=self._callback_wrapper(on_message_callback))
        self.channel.start_consuming()

    def stop_consuming(self):
        self.channel.stop_consuming()

    def send(self, message):
        self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=message)

    def close(self):
        self.connection.close()
    
    def _callback_wrapper(self, on_message_callback):
        def wrapper(ch, method, properties, body):
            ack = lambda:ch.basic_ack(delivery_tag=method.delivery_tag)
            nack = lambda:ch.basic_nack(delivery_tag=method.delivery_tag)
            on_message_callback(body, ack, nack)
        return wrapper

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=exchange_name, exchange_type='topic')
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        self.queue_name = ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))
        self.channel.queue_declare(queue=self.queue_name, exclusive=True)
        for routing_key in routing_keys:
            self.channel.queue_bind(exchange=exchange_name, queue=self.queue_name, routing_key=routing_key)
    
    def send(self, message):
        for routing_key in self.routing_keys:
            self.channel.basic_publish(exchange=self.exchange_name, routing_key=routing_key, body=message)
    
    def start_consuming(self, on_message_callback):
        self.channel.basic_consume(queue=self.queue_name, on_message_callback=self._callback_wrapper(on_message_callback))
        self.channel.start_consuming()

    def stop_consuming(self):
        self.channel.stop_consuming()

    def close(self):
        self.connection.close()

    def _callback_wrapper(self, on_message_callback):
        def wrapper(ch, method, properties, body):
            ack = lambda:ch.basic_ack(delivery_tag=method.delivery_tag)
            nack = lambda:ch.basic_nack(delivery_tag=method.delivery_tag)
            on_message_callback(body, ack, nack)
        return wrapper


    