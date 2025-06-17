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
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))  # Important!
    )

    logging.info("🚀 Kafka consumer started, waiting for messages...")
    for message in consumer:
        try:
            for encoded_frame in message.value.get("frames", []):
                try:
                    image_bytes = base64.b64decode(encoded_frame + "===")  # add padding just in case
                    Image.open(io.BytesIO(image_bytes)).verify()  # verify it's an image
                    process_image(image_bytes)
                except Exception as e:
                    logging.warning(f"⚠️ Skipping invalid image: {e}")
        except Exception as e:
            logging.exception("❌ Failed to process Kafka message")
            

if __name__ == "__main__":
    consume_kafka()
