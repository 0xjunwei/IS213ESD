import requests

OUTSYSTEMS_URL = "https://personal-d2aan8gd.outsystemscloud.com/LoyaltyService/rest/LoyaltyAPI/AddPoints"


def send_to_outsystems(email, booking_id, amount, points):
    payload = {
        "CustomerEmail": email,
        "bookingId": booking_id,
        "Amount": amount,
        "Points": points,
    }

    print("Sending to OutSystems:", payload)

    try:
        response = requests.post(OUTSYSTEMS_URL, json=payload)

        print("Status:", response.status_code)
        print("Response:", response.text)

    except Exception as e:
        print("ERROR:", e)


def add_points(email, points):
    print(f"Adding {points} points to {email}")
    return True


def remove_points(email, points):
    print(f"Removing {points} points from {email}")
    return True


def update_points(email, points):
    print(f"Updating points for {email}: {points}")
    return True
