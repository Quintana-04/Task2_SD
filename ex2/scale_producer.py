import pika
import json
import time
import random

def send_message(num_messages, host, queue_name, username, password):
    frases = [
        "Eres un tonto y no sabes nada.",
        "No seas un bobo, piensa antes de hablar.",
        "Esa puta actitud no ayuda a nadie.",
        "Qué idiota comportamiento has tenido.",
        "No seas un cabron con los demás.",
        "Solo un tonto haría eso.",
        "No te comportes como un bobo.",
        "Eres una puta vergüenza.",
        "No actúes como un idiota.",
        "Deja de ser un cabron por favor."
    ]

    credentials = pika.PlainCredentials(username, password)
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=host, credentials=credentials))
    channel = connection.channel()

    channel.queue_declare(queue=queue_name, durable=False)

    for i in range(num_messages):
        frase = random.choice(frases)
        message = {"frase": frase}
        channel.basic_publish(exchange='',
                              routing_key=queue_name,
                              body=json.dumps(message))
        print(f"Mensaje enviado: {message}")

    connection.close()

def scale_message(start_messages, max_messages, increment_rate, host, queue_name, username, password):
    num_messages = start_messages
    while num_messages <= max_messages:
        print(f"Enviando {num_messages} mensajes...")
        send_message(num_messages, host, queue_name, username, password)
        num_messages += increment_rate
        time.sleep(10)

if __name__ == '__main__':
    RABBITMQ_HOST = '3.84.201.229'
    QUEUE_NAME = 'order_queue'
    USERNAME = 'user'
    PASSWORD = 'password123'
    
    START_MESSAGES = 5
    MAX_MESSAGES = 200
    INCREMENT_RATE = 20

    scale_message(START_MESSAGES, MAX_MESSAGES, INCREMENT_RATE, RABBITMQ_HOST, QUEUE_NAME, USERNAME, PASSWORD)
