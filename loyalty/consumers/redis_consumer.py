import redis
import time
import json

from loyalty.services.loyalty_service import send_to_outsystems

# =========================
# CONFIG
# =========================

STREAM_NAME = "booking_stream"
GROUP_NAME = "loyalty_group"
CONSUMER_NAME = "consumer-1"

# Redis connection
redis_client = redis.Redis(
    host="localhost",  # change to "redis" if using Docker
    port=6379,
    decode_responses=True,
)

# =========================
# CREATE CONSUMER GROUP
# =========================


def create_consumer_group():
    try:
        redis_client.xgroup_create(
            name=STREAM_NAME,
            groupname=GROUP_NAME,
            id="0",  # read from beginning (use "$" if only new messages)
            mkstream=True,
        )
        print("✅ Consumer group created")
    except Exception as e:
        print("ℹ️ Group may already exist:", e)


# =========================
# MAIN CONSUMER LOOP
# =========================


def consume_messages():
    print("🚀 Loyalty consumer started...")

    try:
        while True:
            print("📡 Polling Redis...")

            messages = redis_client.xreadgroup(
                groupname=GROUP_NAME,
                consumername=CONSUMER_NAME,
                streams={STREAM_NAME: ">"},  # only new messages
                block=5000,
            )

            if messages:
                for stream, events in messages:
                    for message_id, raw_data in events:

                        print(f"\n📥 Received event {message_id}: {raw_data}")

                        try:
                            process_event(raw_data)

                            # ✅ ACK ONLY AFTER SUCCESS
                            redis_client.xack(STREAM_NAME, GROUP_NAME, message_id)
                            print(f"✅ ACK message {message_id}")

                        except Exception as e:
                            print("❌ Processing failed:", e)
                            # ❗ no ACK → message will be retried

            else:
                print("⏳ No new messages...")

    except KeyboardInterrupt:
        print("\n🛑 Consumer stopped gracefully.")

    except Exception as e:
        print(f"🔥 Error reading stream: {e}")
        time.sleep(2)


# =========================
# EVENT PROCESSOR
# =========================


def process_event(raw_data):
    try:
        # Redis stores payload inside "data"
        data = json.loads(raw_data["data"])

        print("🔍 Parsed event:", data)

        event_type = data.get("event")

        if event_type == "booking_paid":
            handle_booking_paid(data)

        elif event_type == "booking_cancelled":
            handle_booking_cancelled(data)

        elif event_type == "booking_modified":
            handle_booking_modified(data)

        else:
            print(f"⚠️ Unknown event type: {event_type}")

    except Exception as e:
        print("❌ Error processing event:", e)
        raise e  # 🔥 VERY IMPORTANT for retry


# =========================
# HANDLERS
# =========================


def handle_booking_paid(data):
    email = data.get("email")
    booking_id = data.get("bookingId")
    amount = int(data.get("amount", 0))

    points = amount // 10

    print("➡️ Processing booking_paid")
    print("📤 Calling OutSystems...")

    send_to_outsystems(email, booking_id, amount, points)


def handle_booking_cancelled(data):
    email = data.get("email")
    booking_id = data.get("bookingId")
    amount = int(data.get("amount", 0))

    points = -(amount // 10)  # negative points

    print("➡️ Processing booking_cancelled")
    print("📤 Calling OutSystems...")

    send_to_outsystems(email, booking_id, amount, points)


def handle_booking_modified(data):
    email = data.get("email")
    booking_id = data.get("bookingId")

    old_amount = int(data.get("old_amount", 0))
    new_amount = int(data.get("new_amount", 0))

    diff = (new_amount // 10) - (old_amount // 10)

    print("➡️ Processing booking_modified")
    print("📤 Calling OutSystems...")

    send_to_outsystems(email, booking_id, new_amount, diff)


# =========================
# ENTRY POINT
# =========================

if __name__ == "__main__":
    create_consumer_group()
    consume_messages()
