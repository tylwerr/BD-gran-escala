from VehicleDataGenerator import VehicleDataGenerator
import argparse
import json
import uuid
import io
from minio import Minio
from minio.error import S3Error
from confluent_kafka import Producer, KafkaException

def write_json_to_minio(data, object_name):
    # MinIO configuration
    MINIO_ENDPOINT = 'minio:9000'  # e.g., 'play.min.io' or 'your_minio_server:9000'
    MINIO_ACCESS_KEY = 'minio-root-user'   # Replace with your MinIO access key
    MINIO_SECRET_KEY = 'minio-root-password'   # Replace with your MinIO secret key
    MINIO_SECURE = False # Set to True if using HTTPS
    bucket_name = 'local-bucket'    # The bucket where you want to store the JSON
    # Initialize the MinIO client
    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE
    )

    try:
        # 1. Encode the JSON string to bytes
        # MinIO (and S3) expects bytes for object content
        json_bytes = data.encode('utf-8')

        # 2. Create a BytesIO object from the bytes
        # put_object expects a file-like object
        data_stream = io.BytesIO(json_bytes)
        data_length = len(json_bytes)

        # 3. Ensure the bucket exists (optional, but good practice)
        if not minio_client.bucket_exists(bucket_name):
            print(f"Bucket '{bucket_name}' does not exist. Creating it...")
            minio_client.make_bucket(bucket_name)
            print(f"Bucket '{bucket_name}' created successfully.")
        else:
            print(f"Bucket '{bucket_name}' already exists.")

        # 4. Upload the JSON bytes to MinIO
        minio_client.put_object(
            bucket_name,
            object_name,
            data_stream,
            data_length,
            content_type='application/json' # Set content type for proper serving
        )

        print(f"Successfully wrote JSON to '{bucket_name}/{object_name}'")

    except S3Error as exc:
        print(f"MinIO error occurred: {exc}")
    except Exception as exc:
        print(f"An unexpected error occurred: {exc}")

def delivery_report(err, msg):
    """
    Callback function to handle delivery reports of produced messages.
    Invoked once for each message produced to indicate success or failure.
    """
    if err is not None:
        print(f"Failed to deliver message to topic {msg.topic()} [{msg.partition()}]: {err}")
    else:
        print(f"Message delivered to topic {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
        print(f"Key: {msg.key().decode('utf-8') if msg.key() else 'N/A'}, Value: {msg.value().decode('utf-8')}")


def send_json_to_kafka(json_string, topic, producer):
    """
    Sends a Python dictionary as a JSON string to a Kafka topic.
    """
    try:
        # 1. Encode the JSON string to bytes
        # Kafka expects message values (and keys) to be bytes
        json_bytes = json_string.encode('utf-8')

        # 2. Define a key (optional, but good for partitioning)
        # Using a part of your data as a key can ensure messages with the same key
        # go to the same partition, maintaining order for that key.
        # Make sure the key is also encoded to bytes.
        message_key = str(uuid.uuid4()).encode('utf-8') # Example key

        # 3. Produce the message asynchronously
        # The delivery_report callback will be invoked upon delivery success or failure.
        producer.produce(
            topic=topic,
            key=message_key,
            value=json_bytes,
            callback=delivery_report # Asynchronous send
        )
        print("Attempted to send message")

    except Exception as e:
        print(f"An unexpected error occurred: {e}")


def main():

    parser = argparse.ArgumentParser(description="A Python script to generate random data.")

    # 2. Add arguments
    # --name: A required string argument to specify a name.
    parser.add_argument(
        "--output",
        type=str,
        required=False,
        default="stdout",
        help="The output to write the data. Can be file_path, kafka topic, minio bucket, stdout (by default)"
    )

    # --age: An optional integer argument to specify an age.
    # It has a default value of 30 if not provided.
    parser.add_argument(
        "--num_records",
        type=int,
        default=1,
        help="The amount of records to generate. 0 for infinite generation. Default is 1."
    )

    parser.add_argument(
        "--output_path",
        type=str,
        required=False,
        default="",
        help="The output path to write the data. If output is file_path, this is required."
    )

    # 3. Parse the arguments
    # This line reads the arguments provided by the user from the command line.
    args = parser.parse_args()

    # 4. Access and use the arguments
    output = args.output
    num_records = args.num_records

    print(f"Hello, generating {num_records} records!")
    generator = VehicleDataGenerator(num_records)
    # Perform actions based on the parsed arguments
    if output == "stdout":
        for record in generator:
            print(record)

    elif output == "file_path":
        if args.output_path == "":
            raise ValueError("output_path must be specified when output is file_path.")
        with open(args.output_path, 'w') as file:
            for record in generator:
                file.write(f"{record}\n")
        print(f"Data written to {args.output_path}")
    elif output == "kafka":
        KAFKA_BROKERS = 'kafka:19092'  # Replace with your Kafka broker addresses
        KAFKA_TOPIC = 'systemd_logs'     # Replace with your target Kafka topic
        
        conf = {
            'bootstrap.servers': KAFKA_BROKERS,
            'client.id': 'python-json-producer',
            # 'security.protocol': 'SASL_SSL', # Uncomment for secure clusters
            # 'sasl.mechanisms': 'PLAIN',      # Uncomment for secure clusters
            # 'sasl.username': 'your_username',# Uncomment for secure clusters
            # 'sasl.password': 'your_password' # Uncomment for secure clusters
        }
        producer = Producer(conf)
        for record in generator:
            send_json_to_kafka(record, KAFKA_TOPIC, producer)
        # Wait for any outstanding messages to be delivered and delivery reports to be received
        print("Waiting for messages to be delivered...")
        producer.poll(1)
        # Flush the producer to ensure all messages are sent
        print("Flushing producer...")   
        producer.flush(timeout=10)
        
    elif output == "minio":
        if args.output_path == "":
            raise ValueError("output_path must be specified when output is minio.")

        sample_data = "" 
        for record in generator:
            sample_data += f"{record}\n"

        print(sample_data)
        write_json_to_minio(sample_data, args.output_path)
    

if __name__ == "__main__":
    main()
