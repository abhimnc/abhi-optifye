import torch
import base64
import requests
import io
from kafka import KafkaConsumer
from PIL import Image
from model import model
from utils import preprocess
import logging
import json

def process_image(image_bytes):
    image_tensor = preprocess(image_bytes)
    with torch.no_grad():
        prediction = model([image_tensor])[0]

    data = {
        "boxes": prediction["boxes"].tolist(),
        "labels": prediction["labels"].tolist(),
        "scores": prediction["scores"].tolist(),
        "image": base64.b64encode(image_bytes).decode("utf-8")
    }

    # Send to Flask server for drawing and S3 upload
    response = requests.post("http://flask-server:5555/draw", json=data)
    logging.info("Uploaded to S3:", response.json()["s3_url"])


def consume_kafka():
    consumer = KafkaConsumer(
        "demo-video-stream",
        bootstrap_servers="kafka:9092",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="detector-group",
        # Do not use value_deserializer here
    )

    logging.info("🚀 Kafka consumer started, waiting for messages...")
    for message in consumer:
        try:
            try:
                payload = json.loads(message.value.decode("utf-8"))
            except Exception as e:
                logging.warning(f"⚠️ Invalid JSON in Kafka message: {e}")
                continue

            frames = payload.get("frames", [])
            for encoded_frame in frames:
                try:
                    # Add padding safety
                    missing_padding = len(encoded_frame) % 4
                    if missing_padding:
                        encoded_frame += '=' * (4 - missing_padding)

                    image_bytes = base64.b64decode(encoded_frame)
                    
                    # Basic image validation
                    Image.open(io.BytesIO(image_bytes)).verify()

                    process_image(image_bytes)

                except Exception as e:
                    logging.warning(f"⚠️ Skipping invalid image: {e}")
        except Exception as e:
            logging.exception("❌ Failed to process Kafka message")
            

if __name__ == "__main__":
    consume_kafka()
