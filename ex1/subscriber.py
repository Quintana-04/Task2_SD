import threading
import pika
import json
import boto3
import time

log_file = "logs_lambda.txt"
lambdas_invocadas = 0
lock = threading.Lock()

def invoke_lambda_worker(message):
    lambda_client = boto3.client('lambda', region_name='us-east-1')
    payload = json.dumps({'frase': message.get('frase')})
    try:
        lambda_client.invoke(
            FunctionName="lambda_delivery",
            InvocationType='Event',  # Asíncrono
            Payload=payload
        )
        print("Delivery Lambda invoked for phrase:", message.get('frase'))
        global lambdas_invocadas
        with lock:
            lambdas_invocadas += 1
    except Exception as e:
        print("Error invoking delivery Lambda for phrase:", message.get('frase'), e)

def callback(ch, method, properties, body):
    try:
        message = json.loads(body)
        print("Message received:", message)
        invoke_lambda_worker(message)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print("Error processing the message:", e)
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

def connect_to_rabbit():
    credentials = pika.PlainCredentials('user', 'password123')
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='98.81.175.210', credentials=credentials)  # Cambiar IP
    )
    channel = connection.channel()
    channel.queue_declare(queue='order_queue', durable=False)
    return connection, channel

def monitor(channel, interval=30):
    while True:
        with lock:
            invocadas = lambdas_invocadas
        
        queue_name = 'order_queue'
        q = channel.queue_declare(queue=queue_name, durable=False, passive=True)
        cola_size = q.method.message_count
                
        log_line = (f"[Monitor] Lambdas invocadas: {invocadas} | "
                    f"Tamaño cola RabbitMQ: {cola_size}")
        
        print(log_line)
        with open(log_file, "a") as f:
            f.write(log_line + "\n")
            f.flush()  # Asegura que se escribe inmediatamente
        
        time.sleep(interval)

def start_subscriber():
    connection, channel = connect_to_rabbit()
    
    monitor_thread = threading.Thread(target=monitor, args=(channel,), daemon=True)
    monitor_thread.start()
    
    channel.basic_consume(queue='order_queue', on_message_callback=callback)
    
    print("Subscriber active. Waiting for messages on the 'order_queue'...")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("Manual interruption. Closing subscriber...")
        channel.stop_consuming()
    finally:
        connection.close()

if __name__ == '__main__':
    start_subscriber()
