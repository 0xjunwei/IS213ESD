import requests
import os
from dotenv import load_dotenv

load_dotenv()

OUTSYSTEMS_URL = os.getenv("OUTSYSTEMS_URL")


def send_to_outsystems(email, booking_id, amount, points):
    payload = {
        "CustomerEmail": email,
        "bookingId": booking_id,
        "Amount": amount,
        "Points": points,
    }

    print("📤 Sending to OutSystems:", payload, flush=True)

    if not OUTSYSTEMS_URL:
        print("❌ OUTSYSTEMS_URL not set in .env", flush=True)
        raise ValueError("Missing OUTSYSTEMS_URL")

    try:
        response = requests.post(
            OUTSYSTEMS_URL,
            json=payload,
            timeout=5  # ⏱ prevent hanging
        )

        print("📡 Status:", response.status_code, flush=True)
        print("📨 Response:", response.text, flush=True)

        if response.status_code != 200:
            raise Exception(f"OutSystems returned {response.status_code}")

    except requests.exceptions.Timeout:
        print("⏱ Request timed out", flush=True)
        raise

    except requests.exceptions.RequestException as e:
        print("🌐 Network error:", e, flush=True)
        raise


# Optional local logic (not used but kept for completeness)

def add_points(email, points):
    print(f"Adding {points} points to {email}", flush=True)
    return True


def remove_points(email, points):
    print(f"Removing {points} points from {email}", flush=True)
    return True


def update_points(email, points):
    print(f"Updating points for {email}: {points}", flush=True)
    return True