import torch
from torchvision.models.detection import fasterrcnn_resnet50_fpn

def load_model():
    model = fasterrcnn_resnet50_fpn(pretrained=True)
    model.eval()
    return model

model = load_model()
