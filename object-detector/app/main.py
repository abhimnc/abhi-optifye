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

logging.basicConfig(
    level=logging.INFO,  # or DEBUG for more verbose logs
    format="%(asctime)s - %(levelname)s - %(message)s"
)


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
    logging.info("Uploaded to S3:", response.json())


def consume_kafka():
    logging.info("----inside consume kafka--------...")
    consumer = KafkaConsumer(
        "demo-video-stream",
        bootstrap_servers="kafka:9092",
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="detector-group",
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        
    )

    logging.info("🚀 Kafka consumer started, waiting for messages...")
    for message in consumer:
        # r = message
        # logging.info(f"message {r} received")
        raw_value = message.value
        logging.info(f"Received message with key: {message.key}, value length: {len(raw_value) if raw_value else 0}")
        # logging.info(f"Raw message value: {raw_value}")
        try:
            
            if not raw_value:
                logging.warning("⚠️ Received empty message.")
                continue
            # try:
            #     payload = json.loads(message.value.decode("utf-8"))
            # except Exception as e:
            #     logging.warning(f"⚠️ Invalid JSON in Kafka message: {e},, raw: {raw_value}")
            #     continue

            frames = raw_value
            
            logging.info(f"Received frames: {type(frames)}")
            # for encoded_frame in frames:
            logging.info(f"keys of frames: {frames.keys()}")
            logging.info(f"frameslength ---------------: {len(frames.get('frames'))}:---------------")
            ff = frames.get('frames', [])
            encoded_frame = base64.b64encode(ff).decode("utf-8") # Assuming we only process the first frame for simplicity
            try:
                # Add padding safety
                # missing_padding = len(encoded_frame) % 4
                # if missing_padding:
                #     encoded_frame += '=' * (4 - missing_padding)

                # image_bytes = base64.b64decode(encoded_frame)
                
                # Basic image validation
                # Image.open(io.BytesIO(image_bytes)).verify()

                process_image(encoded_frame)

            except Exception as e:
                logging.warning(f"⚠️ Skipping invalid image: {e}")
        except Exception as e:
            logging.exception("❌ Failed to process Kafka message")
            

if __name__ == "__main__":
    consume_kafka()
