import pika

# Connect to RabbitMQ server
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

# Declare a queue
channel.queue_declare(queue='hello')

print(' [*] Waiting for messages. To exit press CTRL+C')

# Define a callback function for basic_consume
def callback(ch, method, properties, body):
    print(f" [x] Received {body}")

# Set up subscription on the queue
channel.basic_consume(queue='hello', on_message_callback=callback, auto_ack=True)

# Start consuming
channel.start_consuming()
