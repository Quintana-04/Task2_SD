import pika
import json
import time

def send_message(num_messages, host, queue_name, username, password):
    credentials = pika.PlainCredentials(username, password)
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=host, credentials=credentials))
    channel = connection.channel()

    channel.queue_declare(queue=queue_name, durable=False)

    for i in range(num_messages):
        message = {"frase": f"Esta es la frase número {i}"}
        channel.basic_publish(exchange='',
                              routing_key=queue_name,
                              body=json.dumps(message))
        print(f"Mensaje enviado: {message}")
        time.sleep(0.05)  # Pequeña pausa para no saturar demasiado rápido

    connection.close()

if __name__ == '__main__':
    RABBITMQ_HOST = '<EC2_PUBLIC_IP>'
    QUEUE_NAME = 'order_queue'
    USERNAME = 'user'
    PASSWORD = 'password123'
    NUM_MESSAGES = 10  # Número de frases a enviar

    send_message(NUM_MESSAGES, RABBITMQ_HOST, QUEUE_NAME, USERNAME, PASSWORD)
