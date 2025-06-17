from flask import Flask, request, jsonify
from utils import draw_boxes
import boto3
import base64
import io
import uuid
import os
from PIL import Image
import os

app = Flask(__name__)



AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
AWS_REGION = os.environ["AWS_DEFAULT_REGION"]
BUCKET = os.environ["S3_BUCKET_NAME"]
s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

@app.route("/draw", methods=["POST"])
def draw():
    data = request.get_json()
    image_data = base64.b64decode(data["image"])
    boxes = data["boxes"]
    labels = data["labels"]
    scores = data["scores"]

    image = Image.open(io.BytesIO(image_data)).convert("RGB")
    image_with_boxes = draw_boxes(image, boxes, labels, scores)

    # Save to buffer
    buf = io.BytesIO()
    image_with_boxes.save(buf, format="JPEG")
    buf.seek(0)

    # Generate unique key
    image_key = f"processed/{uuid.uuid4().hex}.jpg"

    # Upload to S3
    s3.upload_fileobj(buf, BUCKET, image_key, ExtraArgs={"ContentType": "image/jpeg", "ACL": "public-read"})

    # Create public URL
    url = f"https://{BUCKET}.s3.amazonaws.com/{image_key}"

    return jsonify({"s3_url": url})





if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5555, debug=True)

