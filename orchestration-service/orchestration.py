import os
import time
from concurrent.futures import ThreadPoolExecutor

import redis
import requests
from flask import Flask, jsonify, request

app = Flask(__name__)


# Config (override via env vars as needed)
authService = os.getenv("AUTH_SERVICE", "http://auth-service:5000")
paymentService = os.getenv("PAYMENT_SERVICE", "http://payment-service:5002")
bookingsService = os.getenv("BOOKINGS_SERVICE", "http://bookings-service:5000")
roomsService = os.getenv("ROOMS_SERVICE", "http://rooms-service:5003")
forwardTimeout = float(os.getenv("FORWARD_TIMEOUT", "8"))

redisHost = os.getenv("REDIS_HOST", "localhost")
redisPort = int(os.getenv("REDIS_PORT", "6379"))
redisStream = os.getenv("LOYALTY_STREAM", "booking_events")

# In-memory sessions: token -> user
sessions = {}

# Cache booking context between step-1 and step-2 by payment intent id.
pendingBookings = {}


# Redis client (used for loyalty events)
redis_client = redis.Redis(host=redisHost, port=redisPort, decode_responses=True)


# helper functions
def getToken():
    """
    Get the token from the request headers.
    """
    header = request.headers.get("Authorization", "")
    if header.startswith("Bearer "):
        return header.split(" ", 1)[1].strip()
    return None


def getUser():
    """
    Get the user from the session.
    """
    token = getToken()
    if not token:
        return None
    return sessions.get(token)


def checkAuth():
    """
    Call this at the start of any route that needs auth.
    Return (response, status) if unauthorized, else None.
    """
    user = getUser()
    if not user:
        return jsonify({"error": "Unauthorized"}), 401
    return None


def forward(method, url):
    """
    Minimal JSON forwarder
    forwards query string and JSON body
    returns upstream JSON when possible, else text
    """
    headers = {}
    if method.upper() in ("POST", "PUT", "PATCH"):
        headers["Content-Type"] = "application/json"

    try:
        resp = requests.request(
            method=method.upper(),
            url=url,
            params=request.args,
            json=request.get_json(silent=True),
            headers=headers,
            timeout=forwardTimeout,
        )
    except requests.Timeout:
        return jsonify({"error": "Upstream timeout"}), 504
    except requests.ConnectionError:
        return jsonify({"error": "Upstream unavailable"}), 502

    try:
        data = resp.json()
    except ValueError:
        data = {"message": resp.text}

    return jsonify(data), resp.status_code


# Health (orchestrator)
@app.get("/health")
def health():
    """
    Simple health probe for the orchestrator service.
    """
    return jsonify({"status": "ok", "service": "orchestrator"}), 200


# AUTH
@app.post("/route-auth/login")
def routeAuthLogin():
    """
    Authenticate a user via the auth service and cache the session token.
    """
    payload = request.get_json(silent=True) or {}
    try:
        upstream = requests.post(
            f"{authService}/auth/login",
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=forwardTimeout,
        )
    except requests.Timeout:
        return jsonify({"error": "Auth service timeout"}), 504
    except requests.ConnectionError:
        return jsonify({"error": "Auth service unavailable"}), 502

    try:
        data = upstream.json()
    except ValueError:
        return jsonify({"error": "Invalid response from auth service"}), 502

    if upstream.status_code != 200 or "token" not in data:
        return jsonify(data), upstream.status_code

    token = data["token"]
    sessions[token] = data.get("user", {})
    return jsonify(data), 200


@app.post("/route-auth/register")
def routeAuthRegister():
    """
    Register a new user account through the auth service.
    """
    return forward("POST", f"{authService}/auth/register")


@app.get("/route-auth/health")
def routeAuthHealth():
    """
    Proxy health check call to the auth service.
    """
    return forward("GET", f"{authService}/health")


@app.post("/route-auth/logout")
def routeAuthLogout():
    """
    Log the caller out and drop the in-memory session mapping.
    """
    unauthorized = checkAuth()
    if unauthorized:
        return unauthorized

    token = getToken()
    if token and token in sessions:
        sessions.pop(token, None)
    return jsonify({"message": "logged out"}), 200


# PAYMENT
@app.get("/route-payment/health")
def routePaymentHealth():
    """
    Check that the payment service is reachable for authenticated callers.
    """
    unauthorized = checkAuth()
    if unauthorized:
        return unauthorized
    return forward("GET", f"{paymentService}/health")


@app.post("/route-payment/payments/create-intents")
def routePaymentCreateIntents():
    """
    Create payment intents by forwarding the request to the payment service.
    """
    return forward("POST", f"{paymentService}/payments/create-intents")


@app.post("/route-payment/payments/settle")
def routePaymentSettle():
    """
    Settle a payment intent via the payment service.
    """
    return forward("POST", f"{paymentService}/payments/settle")


@app.post("/route-payment/payments/settle-card")
def routePaymentSettleCard():
    """
    Card-based settlement endpoint forwarded to the payment service.
    """
    return forward("POST", f"{paymentService}/payments/settle-card")



# I need to lock this route to only allow administrator to call this / only call from other function such as cancel booking
# Pending mikhail part
@app.post("/route-payment/payments/refund")
def routePaymentRefund():
    """
    Issue a refund through the payment service for an authenticated user.
    """
    unauthorized = checkAuth()
    if unauthorized:
        return unauthorized
    return forward("POST", f"{paymentService}/payments/refund")


@app.get("/route-payment/payments/<intentId>")
def routePaymentGet(intentId):
    """
    Fetch a single payment intent by ID from the payment service.
    """
    unauthorized = checkAuth()
    if unauthorized:
        return unauthorized
    return forward("GET", f"{paymentService}/payments/{intentId}")


# BOOKINGS
@app.post("/route-bookings/bookings/create")
def routeBookingsCreate():
    """
    Create a new booking record through the bookings service.
    """
    unauthorized = checkAuth()
    if unauthorized:
        return unauthorized
    return forward("POST", f"{bookingsService}/bookings/create")


@app.get("/route-bookings/bookings/<int:booking_id>")
def routeBookingsGet(booking_id: int):
    """
    Return booking details for the given ID from the bookings service.
    """
    unauthorized = checkAuth()
    if unauthorized:
        return unauthorized
    return forward("GET", f"{bookingsService}/bookings/{booking_id}")


@app.delete("/route-bookings/bookings/<int:booking_id>")
def routeBookingsDelete(booking_id: int):
    """
    Delete a booking in the bookings service based on its ID.
    """
    unauthorized = checkAuth()
    if unauthorized:
        return unauthorized
    return forward("DELETE", f"{bookingsService}/bookings/{booking_id}")


@app.post("/route-bookings/bookings/update")
def routeBookingsUpdate():
    """
    Update an existing booking, typically identified by hold or booking id.
    """
    unauthorized = checkAuth()
    if unauthorized:
        return unauthorized
    return forward("POST", f"{bookingsService}/bookings/update")


# Scenario 1 Initial Step for booking a room: create hold and payment intent
@app.post("/route-rooms/book")
def routeRoomsBook():
    """
    Create a room hold, then create a payment intent using holdId and amount.
    """
    payload = request.get_json(silent=True) or {}
    roomType = payload.get("roomType")
    checkIn = payload.get("checkIn")
    checkOut = payload.get("checkOut")
    customerEmail = payload.get("customerEmail")
    customerMobile = payload.get("customerMobile")

    if not roomType or not checkIn or not checkOut or not customerEmail or not customerMobile:
        return jsonify({
            "error": "roomType, checkIn, checkOut, customerEmail and customerMobile are required"
        }), 400

    try:
        hold_resp = requests.post(
            f"{roomsService}/rooms/holds",
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=forwardTimeout,
        )
    except requests.Timeout:
        return jsonify({"error": "Rooms service timeout"}), 504
    except requests.ConnectionError:
        return jsonify({"error": "Rooms service unavailable"}), 502

    try:
        hold_data = hold_resp.json()
    except ValueError:
        hold_data = {"message": hold_resp.text}

    if hold_resp.status_code >= 400:
        return jsonify(hold_data), hold_resp.status_code

    hold_id = hold_data.get("holdId")
    amount = hold_data.get("amount")

    if not hold_id or amount is None:
        return jsonify({"error": "rooms response missing holdId or amount"}), 502

    payment_payload = {
        "holdId": hold_id,
        "amount": amount,
    }

    try:
        payment_resp = requests.post(
            f"{paymentService}/payments/create-intents",
            json=payment_payload,
            headers={"Content-Type": "application/json"},
            timeout=forwardTimeout,
        )
    except requests.Timeout:
        return jsonify({"error": "Payment service timeout"}), 504
    except requests.ConnectionError:
        return jsonify({"error": "Payment service unavailable"}), 502

    try:
        payment_data = payment_resp.json()
    except ValueError:
        payment_data = {"message": payment_resp.text}

    if payment_resp.status_code < 400:
        intent_id = payment_data.get("paymentIntentId")
        if intent_id:
            pendingBookings[intent_id] = {
                "holdId": hold_id,
                "roomID": hold_data.get("roomID"),
                "roomType": hold_data.get("roomType"),
                "checkIn": hold_data.get("checkIn"),
                "checkOut": hold_data.get("checkOut"),
                "amount": amount,
                "customerEmail": customerEmail,
                "customerMobile": customerMobile,
            }

    return jsonify(payment_data), payment_resp.status_code


# Scenario 1: step 2, after payment is successful, we can create the booking record and finalize the hold
@app.post("/route-rooms/book/confirm")
def routeRoomsConfirmBooking():
    """
    Settle payment, then concurrently create booking and reserve room.
    """
    payload = request.get_json(silent=True) or {}

    intent_id = payload.get("intentID") or payload.get("paymentIntentId")
    tx_hash = payload.get("TxnID_Proof") or payload.get("tx_hash")
    hold_id_input = payload.get("holdID") or payload.get("holdId")

    if not intent_id or not tx_hash or not hold_id_input:
        return jsonify({"error": "intentID, TxnID_Proof and holdID are required"}), 400

    settle_payload = {
        "paymentIntentId": intent_id,
        "tx_hash": tx_hash,
    }

    try:
        settle_resp = requests.post(
            f"{paymentService}/payments/settle",
            json=settle_payload,
            headers={"Content-Type": "application/json"},
            timeout=forwardTimeout,
        )
    except requests.Timeout:
        return jsonify({"error": "Payment service timeout"}), 504
    except requests.ConnectionError:
        return jsonify({"error": "Payment service unavailable"}), 502

    try:
        settle_data = settle_resp.json()
    except ValueError:
        settle_data = {"message": settle_resp.text}

    if settle_resp.status_code >= 400:
        return jsonify({"settle": settle_data}), settle_resp.status_code

    if (settle_data.get("status") or "").upper() != "PAID":
        return jsonify({"settle": settle_data, "error": "payment not marked as PAID"}), 400

    booking_context = pendingBookings.get(intent_id, {})

    hold_id = hold_id_input
    cached_hold_id = booking_context.get("holdId")
    if cached_hold_id and cached_hold_id != hold_id:
        return jsonify({
            "error": "holdID does not match booking context for this paymentIntentId",
            "expectedHoldId": cached_hold_id,
            "providedHoldId": hold_id,
        }), 400

    amount_spent = booking_context.get("amount") or payload.get("amount")
    provided_booking_id = payload.get("bookingId")
    if provided_booking_id is not None:
        try:
            booking_id = int(provided_booking_id)
        except (TypeError, ValueError):
            return jsonify({"error": "bookingId must be an integer"}), 400
    else:
        # Keep id inside signed INT range used by downstream MySQL schemas.
        booking_id = int(time.time() * 1000) % 2147483647

    booking_payload = {
        "id": booking_id,
        "roomID": booking_context.get("roomID") or payload.get("roomID"),
        "roomType": booking_context.get("roomType") or payload.get("roomType"),
        "customerEmail": booking_context.get("customerEmail") or payload.get("customerEmail"),
        "customerMobile": booking_context.get("customerMobile") or payload.get("customerMobile"),
        "checkIn": booking_context.get("checkIn") or payload.get("checkIn"),
        "checkOut": booking_context.get("checkOut") or payload.get("checkOut"),
        "amountSpent": amount_spent,
        "holdId": hold_id,
    }

    missing_booking_fields = [
        key for key in [
            "roomID",
            "roomType",
            "customerEmail",
            "customerMobile",
            "checkIn",
            "checkOut",
            "amountSpent",
            "holdId",
        ] if booking_payload.get(key) in (None, "")
    ]
    if missing_booking_fields:
        return jsonify({
            "error": "Missing booking data for finalize step",
            "missingFields": missing_booking_fields,
            "hint": "Pass the fields in /route-rooms/book or include them in this confirm request",
        }), 400

    room_update_payload = {
        "holdId": hold_id,
        "status": payload.get("roomStatus", "reserved"),
        "bookingId": booking_id,
        "reservationDate": payload.get("reservationDate"),
    }

    def post_json(url, body):
        try:
            response = requests.post(
                url,
                json=body,
                headers={"Content-Type": "application/json"},
                timeout=forwardTimeout,
            )
            try:
                body_data = response.json()
            except ValueError:
                body_data = {"message": response.text}
            return response.status_code, body_data
        except requests.Timeout:
            return 504, {"error": "Upstream timeout"}
        except requests.ConnectionError:
            return 502, {"error": "Upstream unavailable"}

    with ThreadPoolExecutor(max_workers=2) as executor:
        bookings_future = executor.submit(
            post_json,
            f"{bookingsService}/bookings/create",
            booking_payload,
        )
        rooms_future = executor.submit(
            post_json,
            f"{roomsService}/rooms/update",
            room_update_payload,
        )

        bookings_status, bookings_data = bookings_future.result()
        rooms_status, rooms_data = rooms_future.result()

    if bookings_status >= 400 or rooms_status >= 400:
        return jsonify({
            "settle": settle_data,
            "bookings": {"statusCode": bookings_status, "data": bookings_data},
            "rooms": {"statusCode": rooms_status, "data": rooms_data},
            "error": "Finalize step failed after payment settlement",
        }), 502

    try:
        points = int(float(booking_payload["amountSpent"])) // 10
        redis_client.xadd(
            redisStream,
            {
                "event": "add_points",
                "email": booking_payload["customerEmail"],
                "points": str(points),
            },
        )
    except Exception:
        points = None

    pendingBookings.pop(intent_id, None)

    return jsonify({
        "message": "Booking finalized successfully",
        "settle": settle_data,
        "booking": bookings_data,
        "room": rooms_data,
        "loyalty": {
            "event": "add_points",
            "email": booking_payload["customerEmail"],
            "points": points,
        },
    }), 200




@app.post("/route-rooms/rooms/holds")
@app.post("/rooms/holds")
def routeRoomsCreateHold():
    """
    Directly proxy hold creation to the rooms service.
    """
    payload = request.get_json(silent=True) or {}
    roomType = payload.get("roomType")
    checkIn = payload.get("checkIn")
    checkOut = payload.get("checkOut")

    if not roomType or not checkIn or not checkOut:
        return jsonify({"error": "roomType, checkIn and checkOut are required"}), 400

    return forward("POST", f"{roomsService}/rooms/holds")


@app.post("/route-rooms/rooms/update")
@app.post("/rooms/update")
def routeRoomsUpdatePost():
    """
    Update room status/details by forwarding to the rooms service.
    """
    unauthorized = checkAuth()
    if unauthorized:
        return unauthorized

    payload = request.get_json(silent=True) or {}
    holdId = payload.get("holdId")
    status = payload.get("status")

    if not holdId:
        return jsonify({"message": "Missing field: holdId"}), 400
    if not status:
        return jsonify({"message": "Missing field: status"}), 400

    return forward("POST", f"{roomsService}/rooms/update")


@app.put("/route-rooms/rooms/update")
@app.put("/rooms/update")
def routeRoomsUpdatePut():
    """
    Support PUT update calls while routing to the current rooms update handler.
    """
    unauthorized = checkAuth()
    if unauthorized:
        return unauthorized

    payload = request.get_json(silent=True) or {}
    holdId = payload.get("holdId")
    status = payload.get("status")

    if not holdId:
        return jsonify({"message": "Missing field: holdId"}), 400
    if not status:
        return jsonify({"message": "Missing field: status"}), 400

    # rooms-service currently exposes /rooms/update as POST.
    return forward("POST", f"{roomsService}/rooms/update")


@app.post("/route-rooms/rooms/update-reservation")
@app.post("/rooms/update-reservation")
def routeRoomsUpdateReservation():
    """
    Move reservation from one hold to another in the rooms service.
    """
    unauthorized = checkAuth()
    if unauthorized:
        return unauthorized

    payload = request.get_json(silent=True) or {}
    oldHoldId = payload.get("oldHoldId")
    newHoldId = payload.get("newHoldId")

    if not oldHoldId or not newHoldId:
        return jsonify({"message": "oldHoldId and newHoldId are required"}), 400

    return forward("POST", f"{roomsService}/rooms/update-reservation")


# LOYALTY
@app.post("/route-loyalty/add-points")
def routeLoyaltyAddPoints():
    """
    Add loyalty points for the current user by calling the loyalty service.
    """
    unauthorized = checkAuth()
    if unauthorized:
        return unauthorized

    payload = request.get_json(silent=True) or {}
    email = payload.get("email")
    points = payload.get("points")

    if not email or points is None:
        return jsonify({"error": "email and points are required"}), 400

    redis_client.xadd(
        redisStream,
        {
            "event": "add_points",
            "email": email,
            "points": str(points),
        },
    )

    return jsonify({"status": "queued", "event": "add_points"}), 202


@app.post("/route-loyalty/deduct-points")
def routeLoyaltyDeductPoints():
    """
    Deduct loyalty points for the current user via the loyalty service.
    """
    unauthorized = checkAuth()
    if unauthorized:
        return unauthorized

    payload = request.get_json(silent=True) or {}
    email = payload.get("email")
    points = payload.get("points")

    if not email or points is None:
        return jsonify({"error": "email and points are required"}), 400

    redis_client.xadd(
        redisStream,
        {
            "event": "deduct_points",
            "email": email,
            "points": str(points),
        },
    )

    return jsonify({"status": "queued", "event": "deduct_points"}), 202


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
