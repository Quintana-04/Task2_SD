import json
import time
import pika

RABBITMQ_USER = 'user'
RABBITMQ_PASS = 'password123'
RABBITMQ_HOST = '3.84.201.229'  # Dirección IP de RabbitMQ

insults = ["tonto", "bobo", "puta", "idiota", "cabron"]

def connect_to_rabbitmq():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        credentials=credentials,
        heartbeat=600,
        blocked_connection_timeout=300
    )
    return pika.BlockingConnection(parameters)

def filter_phrase(frase):
    for insult in insults:
        frase = frase.replace(insult, "CENSORED")
    return frase

def lambda_handler(event, context):
    connection = None
    channel = None
    try:
        connection = connect_to_rabbitmq()
        channel = connection.channel()
        channel.queue_declare(queue='order_queue', durable=False)

        while True:
            try:
                method_frame, header_frame, body = channel.basic_get(queue='order_queue', auto_ack=False)

                if method_frame:
                    message = json.loads(body)
                    frase = message.get('frase')

                    if frase == "STOP":
                        print("Received STOP signal. Exiting worker.")
                        channel.basic_ack(method_frame.delivery_tag)
                        break

                    filtered = filter_phrase(frase)
                    print(f"Filtered phrase: {filtered}")
                    channel.basic_ack(method_frame.delivery_tag)
                else:
                    time.sleep(1)

            except Exception as e:
                print(f"Error procesando mensaje: {e}")
                time.sleep(1)

    except Exception as e:
        print(f"Error in Lambda worker: {e}")
        raise
    finally:
        if channel and channel.is_open:
            channel.close()
        if connection and connection.is_open:
            connection.close()

    return {
        "statusCode": 200,
        "body": json.dumps({"message": "Worker finished"})
    }
