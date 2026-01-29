import json
import os
from glob import glob
from google.cloud import pubsub_v1

# Authentication setup
files = glob("my-first-project231-*.json") 
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

# Project and subscription configuration
project_id = "my-first-project231"
subscription_name = "design-sub"

# Initialize Pub/Sub subscriber
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_name)

# Start listening for messages
print(f"Listening for messages on {subscription_path}...\n")

# Callback function to process received messages
def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    
    try:
        
        # Decode message data and convert JSON string to dictionary
        record_dict = json.loads(message.data.decode("utf-8"));
        print("--Record--")
        for key, value in record_dict.items():
            print(f"{key}: {value}")

        message.ack()
    except Exception as e:
        print(f"Failed to process message: {e}")
        message.nack()

with subscriber:
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()

