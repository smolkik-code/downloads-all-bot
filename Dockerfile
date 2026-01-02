FROM python:3.11-slim

RUN apt update && apt install -y \
    ffmpeg \
    wireguard \
    iproute2 \
    iptables \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /DownloadSM_2

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY bot ./bot

CMD ["python", "bot/main.py"]
