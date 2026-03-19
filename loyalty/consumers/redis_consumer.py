import redis
import time
import json

from services.loyalty_service import send_to_outsystems

# Redis connection
redis_client = redis.Redis(
    host="localhost",
    port=6379,
    decode_responses=True,
)

STREAM_NAME = "booking_stream"  # ✅ FIXED


def consume_messages():
    print("Loyalty consumer started...")

    last_id = "$"  # start from new messages

    try:
        while True:
            print("Polling Redis...")

            messages = redis_client.xread({STREAM_NAME: last_id}, block=5000)

            if messages:
                for stream, events in messages:
                    for message_id, raw_data in events:

                        print(f"\nReceived event {message_id}: {raw_data}")

                        # ✅ process first (safer)
                        process_event(raw_data)

                        # ✅ update AFTER processing
                        last_id = message_id

            else:
                print("No new messages...")

    except KeyboardInterrupt:
        print("\nConsumer stopped gracefully.")

    except Exception as e:
        print(f"Error reading stream: {e}")
        time.sleep(2)


def process_event(raw_data):
    try:
        # ✅ IMPORTANT: parse JSON from "data"
        data = json.loads(raw_data["data"])

        print("Parsed event:", data)

        event_type = data.get("event")

        if event_type == "booking_paid":
            handle_booking_paid(data)

        elif event_type == "booking_cancelled":
            handle_booking_cancelled(data)

        elif event_type == "booking_modified":
            handle_booking_modified(data)

        else:
            print(f"Unknown event type: {event_type}")

    except Exception as e:
        print("Error processing event:", e)


# =========================
# HANDLERS
# =========================


def handle_booking_paid(data):
    email = data.get("email")
    booking_id = data.get("bookingId")
    amount = int(data.get("amount", 0))

    points = amount // 10
    print("Calling OutSystems now...")

    send_to_outsystems(email, booking_id, amount, points)


def handle_booking_cancelled(data):
    email = data.get("email")
    booking_id = data.get("bookingId")
    amount = int(data.get("amount", 0))

    points = -(amount // 10)  # ✅ negative
    print("Calling OutSystems now...")

    send_to_outsystems(email, booking_id, amount, points)


def handle_booking_modified(data):
    email = data.get("email")
    booking_id = data.get("bookingId")

    old_amount = int(data.get("old_amount", 0))
    new_amount = int(data.get("new_amount", 0))

    diff = (new_amount // 10) - (old_amount // 10)
    print("Calling OutSystems now...")

    send_to_outsystems(email, booking_id, new_amount, diff)


if __name__ == "__main__":
    consume_messages()
