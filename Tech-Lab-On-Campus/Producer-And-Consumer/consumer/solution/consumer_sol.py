from consumer_interface import mqConsumerInterface 
import pika
import os

class mqConsumer(mqConsumerInterface):

    def __init__(self, binding_key: str, exchange_name: str, queue_name: str):
          # Save parameters to class variables
         self.key = binding_key
         self.nameOfExchange = exchange_name
         self.nameOfQueue = queue_name

         # Call setupRMQConnection
         self.setupRMQConnection()
    
    def setupRMQConnection(self):
         # Set-up Connection to RabbitMQ service
         con_params = pika.URLParameters(os.environ["AMQP_URL"])
         self.connection = pika.BlockingConnection(parameters=con_params)

         # Establish Channel
         self.channel = self.connection.channel()

         # Create Queue if not already present
         self.channel.queue_declare(queue = self.nameOfQueue)

         # Create the exchange if not already present
         exchange = self.channel.exchange_declare(exchange= self.nameOfExchange)

         # Bind Binding Key to Queue on the exchange
         self.channel.queue_bind(queue= self.nameOfQueue, routing_key= self.key, exchange= self.nameOfExchange)

         # Set-up Callback function for receiving messages
         self.channel.basic_consume(self.nameOfQueue, self.on_message_callback, auto_ack=False)
         
    def on_message_callback(self, channel, method_frame, header_frame, body):
          # Acknowledge message
          channel.basic_ack(method_frame.delivery_tag, False)
          #Print message (The message is contained in the body parameter variable)
          print(f"Received Message {body}")

    def startConsuming(self):
         # Print " [*] Waiting for messages. To exit press CTRL+C"
         print(" [*] Waiting for messages. To exit press CTRL+C")
         
         # Start consuming messages
         self.channel.start_consuming()
         
    def __del__(self):
        # Print "Closing RMQ connection on destruction"
        print("Closing RMQ connection on destruction")

        # Close Channel
        self.channel.close()

        # Close Connection
        self.connection.close()


    