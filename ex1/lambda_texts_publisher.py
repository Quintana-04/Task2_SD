import boto3
import csv
import io
import json
import pika
from urllib.parse import unquote
from botocore.exceptions import ClientError

def lambda_handler(event, context):
    try:
        # Extract bucket and key from the S3 event and decode the key
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = unquote(event['Records'][0]['s3']['object']['key'])
        print(f"Processing object: {key} from bucket: {bucket}")
        
        # Read the CSV file from S3
        s3 = boto3.client('s3')
        try:
            response = s3.get_object(Bucket=bucket, Key=key)
        except ClientError as e:
            if e.response['Error']['Code'] == "NoSuchKey":
                error_message = f"Object with key '{key}' not found in bucket '{bucket}'."
                print(error_message)
                return {
                    "statusCode": 404,
                    "body": error_message
                }
            else:
                raise
        
        csv_content = response['Body'].read().decode('utf-8')
        print("CSV content read from S3:")
        print(csv_content)
        
        # Parse CSV file using csv.DictReader
        csv_file = io.StringIO(csv_content)
        reader = csv.DictReader(csv_file)
        
        # Connect to RabbitMQ (replace 'PUBLIC_IP' with your RabbitMQ server's IP, and update credentials)
        credentials = pika.PlainCredentials('user', 'password123')
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host='<EC2_PUBLIC_IP>',
                credentials=credentials
            )
        )
        channel = connection.channel()
        channel.queue_declare(queue='order_queue')
        
        # Process each order in the CSV and publish to RabbitMQ
        for row in reader:
            frase = { 'frase': row.get('Frase') }
            print("Processing text:", frase)
            message = json.dumps(frase)
            channel.basic_publish(exchange='', routing_key='order_queue', body=message)
        
        connection.close()
        return {
            "statusCode": 200,
            "body": "Orders published to RabbitMQ"
        }
    
    except Exception as e:
        print("Error processing order CSV: ", str(e))
        return {
            "statusCode": 500,
            "body": "Error publishing orders: " + str(e)
        }
