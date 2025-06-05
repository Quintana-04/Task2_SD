import threading
import pika
import json
import boto3
import time
import math

log_file = "logs_lambda.txt"
lambdas_invocadas = 0
lock = threading.Lock()

TARGET_RESPONSE_TIME = 5
CAPACITY_SINGLE_WORKER = 10
MAX_WORKERS = 20

prev_backlog = 0
prev_time = time.time()

lambda_client = boto3.client('lambda', region_name='us-east-1')

consuming = True

def connect_to_rabbit(host='3.84.201.229', user='user', password='password123', queue_name='order_queue'):
    credentials = pika.PlainCredentials(user, password)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=host, credentials=credentials)
    )
    channel = connection.channel()
    channel.queue_declare(queue=queue_name, durable=False)
    print("Connected to RabbitMQ.")
    return connection, channel

def invoke_lambda_worker(function_name="lambda_delivery2"):
    try:
        lambda_client.invoke(
            FunctionName=function_name,
            InvocationType='Event',
            Payload=json.dumps({})
        )
        with lock:
            global lambdas_invocadas
            lambdas_invocadas += 1
        print(f"Lambda worker '{function_name}' invoked.")
    except Exception as e:
        print(f"Error invoking Lambda: {e}")

def send_stop_message(channel, queue_name='order_queue'):
    stop_message = {"frase": "STOP"}
    channel.basic_publish(
        exchange='',
        routing_key=queue_name,
        body=json.dumps(stop_message)
    )
    print("Sent STOP message to queue to stop workers.")

def get_queue_size(channel, queue_name='order_queue'):
    q = channel.queue_declare(queue=queue_name, durable=False, passive=True)
    return q.method.message_count

def process_stream(channel, max_workers=MAX_WORKERS,
                               target_response_time=TARGET_RESPONSE_TIME,
                               capacity_worker=CAPACITY_SINGLE_WORKER,
                               interval=10):
    global prev_backlog, prev_time, lambdas_invocadas, consuming

    current_workers = 0

    while consuming:
        backlog = get_queue_size(channel)
        current_time = time.time()
        delta_t = current_time - prev_time if prev_time else 1

        arrival_rate = max((backlog - prev_backlog) / delta_t, 0)

        n_workers = (backlog + arrival_rate * target_response_time) / capacity_worker
        n_workers = math.ceil(n_workers)
        n_workers = min(max_workers, max(1, n_workers))

        print(f"[Scaler] Backlog={backlog} ArrivalRate={arrival_rate:.2f} msg/s "
              f"-> Lanzando {n_workers} workers (actual {current_workers})")

        if n_workers > current_workers:
            to_launch = n_workers - current_workers
            print(f"[Scaler] Lanzando {to_launch} workers Lambda")
            for _ in range(to_launch):
                invoke_lambda_worker()
            current_workers = n_workers
        elif n_workers < current_workers:
            to_stop = current_workers - n_workers
            print(f"[Scaler] Hay {to_stop} workers de más. Enviando señales STOP.")
            for _ in range(to_stop):
                send_stop_message(channel)
            current_workers = n_workers

        prev_backlog = backlog
        prev_time = current_time

        time.sleep(interval)

def monitor(channel, interval=30):
    global lambdas_invocadas, consuming
    while consuming:
        with lock:
            invocadas = lambdas_invocadas

        backlog = get_queue_size(channel)

        log_line = (f"[Monitor] Lambdas invocadas: {invocadas} | "
                    f"Tamaño cola RabbitMQ: {backlog}")
        print(log_line)

        with open(log_file, "a") as f:
            f.write(log_line + "\n")
            f.flush()

        time.sleep(interval)

def callback(ch, method, properties, body):
    message = json.loads(body)
    print(f"[Callback] Mensaje recibido: {message}")

def read_stream(channel, queue_name='order_queue'):
    print(f"Reading stream from '{queue_name}'...")
    channel.basic_consume(
        queue=queue_name,
        on_message_callback=callback,
        auto_ack=True
    )
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("Interrupted stream reading.")

def start_subscriber():
    global consuming
    connection, channel = connect_to_rabbit()

    monitor_thread = threading.Thread(target=monitor, args=(channel,), daemon=True)
    scaler_thread = threading.Thread(target=process_stream, args=(channel,), daemon=True)

    monitor_thread.start()
    scaler_thread.start()

    print("Dynamic scaler started. Monitoring and scaling...")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Manual interruption. Shutting down...")
        consuming = False
        if connection and connection.is_open:
            connection.close()

if __name__ == '__main__':
    start_subscriber()
