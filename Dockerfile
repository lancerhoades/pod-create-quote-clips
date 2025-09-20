FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

COPY handler.py /app/

CMD ["python", "-u", "handler.py"]
