import time
import os
import csv
from glob import glob
import json
from google.cloud import pubsub_v1

# Authentication setup
files = glob("my-first-project231-*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

#Project and topic configuration
PROJECT_ID = "my-first-project231"
TOPIC_NAME = "design"  
CSV_FILE_PATH = "Labels.csv"

# Initialize Pub/Sub publisher
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_NAME)
print(f"Publishing messages to {topic_path}")

# Read CSV and publish messages
with open(CSV_FILE_PATH, newline="", encoding="utf-8") as csvfile:
    reader = csv.DictReader(csvfile)

# Iterate through each row in the CSV
    for row in reader:
        record_dict = dict(row) # Convert OrderedDict to regular dict

        #Convert dict to JSON string and then to bytes
        message_bytes = json.dumps(record_dict).encode("utf-8")

        # Publish the message
        try:
            future = publisher.publish(topic_path, message_bytes)
            message_id = future.result()  # wait for confirmation
            print(f"Published message ID {message_id}: {record_dict}")

        except Exception as e:
            print(f"Failed to publish {record_dict}: {e}")

        time.sleep(1)  # small delay so you can see messages flowing
