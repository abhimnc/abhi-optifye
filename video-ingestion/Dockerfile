FROM ubuntu:22.04

# Install dependencies: ffmpeg, wget, tar
RUN apt update && \
    apt install -y ffmpeg wget tar && \
    apt clean

WORKDIR /app

# Download and install MediaMTX
RUN wget https://github.com/bluenviron/mediamtx/releases/download/v1.12.3/mediamtx_v1.12.3_linux_amd64.tar.gz && \
    tar -xzf mediamtx_v1.12.3_linux_amd64.tar.gz && \
    mv mediamtx /usr/local/bin && \
    rm mediamtx_v1.12.3_linux_amd64.tar.gz

# Copy your MediaMTX config and video file (optional: if you're using these in the container)
COPY mediamtx.yml .
COPY video.mp4 .

# Expose ports used by MediaMTX
EXPOSE 8554 1935 8888 8189/udp 8000/udp 8001/udp 8890/udp

# Run MediaMTX with your config
CMD ["mediamtx", "mediamtx.yml"]
