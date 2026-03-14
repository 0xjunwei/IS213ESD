import redis
import time
from app.services.loyalty_service import add_points, remove_points, update_points

# Redis connection
redis_client = redis.Redis(
    host="localhost", # change to redis if in docker, else local host 
    port=6379,
    decode_responses=True
)

STREAM_NAME = "booking_events"


def consume_messages():
    print("Loyalty consumer started...")

    while True:
        try:
            messages = redis_client.xread(
                {STREAM_NAME: "$"},
                block=5000
            )

            if messages:
                for stream, events in messages:
                    for message_id, data in events:
                        print(f"Received event {message_id}: {data}")
                        process_event(data)

        except Exception as e:
            print(f"Error reading stream: {e}")
            time.sleep(2)


def process_event(data):

    event_type = data.get("event")
    print(f"Processing event type: {event_type}")

    if event_type == "booking_paid":
        handle_booking_paid(data)

    elif event_type == "booking_cancelled":
        handle_booking_cancelled(data)

    elif event_type == "booking_modified":
        handle_booking_modified(data)

    else:
        print(f"Unknown event type: {event_type}")


def handle_booking_paid(data):

    email = data.get("email")
    amount = int(data.get("amount", 0))

    points = amount // 10

    add_points(email, points)


def handle_booking_cancelled(data):

    email = data.get("email")
    points = int(data.get("points", 0))

    remove_points(email, points)


def handle_booking_modified(data):

    email = data.get("email")
    amount = int(data.get("amount", 0))

    points = amount // 10

    update_points(email, points)


if __name__ == "__main__":
    consume_messages()