
import pika
import os
# import consumer_interface.mqConsumerInterface
from consumer_interface import mqConsumerInterface

class mqConsumer(mqConsumerInterface):
    def __init__ (self, binding_key, exchange_name: str, queue_name: str) -> None:
        self.exchange_name = exchange_name
        self.binding_key = binding_key
        self.queueName = queue_name
        self.setupRMQConnection()

    def setupRMQConnection(self): 
        # We'll first set up the connection and channel
        # connection = pika.BlockingConnection(
        #     pika.ConnectionParameters(host='localhost'))
        # declare a queue and exchange
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        self.connection = pika.BlockingConnection(parameters=con_params)
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queueName)
        exchange = self.channel.exchange_declare(exchange=self.exchange_name)
        #bind the binding key to the queue on the exchange
        self.channel.queue_bind(
            queue = self.queueName,
            routing_key = self.binding_key,
            exchange = self.exchange_name,
        )
        #set up a callback function for receiving messages
        self.channel.basic_consume(
            self.queueName, self.onMessageCallback, auto_ack=False
        )

    def onMessageCallback(self, channel, method_frame, header_frame, body):
        # Acknowledge And Print Message
        self.channel.basic_ack(method_frame.delivery_tag, False)
        print("Received msg")
        print(body)
        

    def startConsuming(self):
        print("starting consuming")
        self.channel.start_consuming()

    def Del(self):
        self.channel.close()
        self.connection.close()



    