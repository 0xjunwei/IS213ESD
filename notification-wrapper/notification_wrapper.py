import os
import time
import redis
import requests

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
STREAM_NAME = os.getenv("STREAM_NAME", "email_stream")
GROUP_NAME = os.getenv("GROUP_NAME", "email_group")
CONSUMER_NAME = os.getenv("CONSUMER_NAME", "consumer_1")

RESEND_API_KEY = os.getenv("RESEND_API_KEY")
RESEND_FROM_EMAIL = os.getenv("RESEND_FROM_EMAIL")

def get_redis_client():
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True,
        socket_keepalive=True,
        health_check_interval=30,
    )


def create_group(client):
    try:
        client.xgroup_create(STREAM_NAME, GROUP_NAME, id="0", mkstream=True)
        print(f"Consumer group '{GROUP_NAME}' created")
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" in str(e):
            print("Consumer group already exists")
        else:
            raise


def send_to_resend(fields):
    to_email = fields.get("to")
    subject = fields.get("subject")
    html = fields.get("html")
    text = fields.get("text")

    payload = {
        "from": RESEND_FROM_EMAIL,
        "to": [to_email],
        "subject": subject,
    }

    if html:
        payload["html"] = html
    if text:
        payload["text"] = text

    response = requests.post(
        "https://api.resend.com/emails",
        headers={
            "Authorization": f"Bearer {RESEND_API_KEY}",
            "Content-Type": "application/json"
        },
        json=payload,
        timeout=30
    )

    response.raise_for_status()
    print("Sent:", response.json())


def consume():
    client = get_redis_client()
    create_group(client)

    while True:
        try:
            messages = client.xreadgroup(
                groupname=GROUP_NAME,
                consumername=CONSUMER_NAME,
                streams={STREAM_NAME: ">"},
                count=1,
                block=5000,
            )
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as e:
            print("Redis disconnected, reconnecting:", e)
            time.sleep(2)
            try:
                client = get_redis_client()
                create_group(client)
            except Exception as reconnect_error:
                print("Redis reconnect failed:", reconnect_error)
                time.sleep(2)
            continue

        if not messages:
            continue

        for stream_name, entries in messages:
            for message_id, fields in entries:
                try:
                    print("Received:", message_id, fields)
                    send_to_resend(fields)
                    client.xack(STREAM_NAME, GROUP_NAME, message_id)
                    print("Acknowledged:", message_id)
                except Exception as e:
                    print("Error processing message:", e)


if __name__ == "__main__":
    consume()