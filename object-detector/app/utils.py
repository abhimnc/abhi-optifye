from PIL import Image
import torchvision.transforms as T
import io

transform = T.Compose([
    T.ToTensor()
])

def preprocess(image_bytes):
    image = Image.open(io.BytesIO(image_bytes)).convert("RGB")
    return transform(image)
