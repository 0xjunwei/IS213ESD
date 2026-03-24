import os
import time
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

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
redisStream = os.getenv("LOYALTY_STREAM", "booking_stream")
notificationStream = os.getenv("NOTIFICATION_STREAM", "email_stream")
redisFallbackEnabled = os.getenv("REDIS_FALLBACK_ENABLED", "true").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
redisHostCandidates = [
    host.strip()
    for host in os.getenv("REDIS_HOST_CANDIDATES", "").split(",")
    if host.strip()
]

# In-memory sessions: token -> user
sessions = {}

# Cache booking context between step-1 and step-2 by payment intent id.
pendingBookings = {}

# Cache hold context for card-based settle flow keyed by holdId.
pendingCardBookings = {}


# Redis client (used for loyalty + notification events)
redis_client = None


def get_redis_client():
    """
    Resolve and cache a reachable Redis client across common Docker host setups.
    """
    global redis_client
    if redis_client is not None:
        return redis_client

    host_candidates = []
    if redisHost:
        host_candidates.append(redisHost)

    if redisFallbackEnabled:
        host_candidates.extend(redisHostCandidates)
        host_candidates.extend(["host.docker.internal", "localhost", "redis"])

    seen = set()
    unique_hosts = []
    for host in host_candidates:
        if host not in seen:
            seen.add(host)
            unique_hosts.append(host)

    last_error = None
    for host in unique_hosts:
        try:
            candidate = redis.Redis(host=host, port=redisPort, decode_responses=True)
            candidate.ping()
            redis_client = candidate
            return redis_client
        except Exception as exc:
            last_error = exc

    raise ConnectionError(f"Unable to connect to Redis on port {redisPort}: {last_error}")


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


def request_json(method, url, payload=None, params=None):
    """
    Perform an upstream request and normalize JSON/text responses.
    """
    headers = {}
    if method.upper() in ("POST", "PUT", "PATCH"):
        headers["Content-Type"] = "application/json"

    try:
        resp = requests.request(
            method=method.upper(),
            url=url,
            json=payload,
            params=params,
            headers=headers,
            timeout=forwardTimeout,
        )
    except requests.Timeout:
        return 504, {"error": "Upstream timeout"}
    except requests.ConnectionError:
        return 502, {"error": "Upstream unavailable"}

    try:
        data = resp.json()
    except ValueError:
        data = {"message": resp.text}

    return resp.status_code, data


def get_hold_context_from_rooms(hold_id):
    """
    Resolve hold context from rooms service so settle-card can still finalize after restarts.
    """
    if not hold_id:
        return {}

    status_code, rooms_data = request_json("GET", f"{roomsService}/rooms")
    if status_code >= 400 or not isinstance(rooms_data, list):
        return {}

    hold_id_str = str(hold_id)
    for room in rooms_data:
        if str(room.get("holdId") or "") != hold_id_str:
            continue

        return {
            "holdId": hold_id_str,
            "roomID": room.get("roomID"),
            "roomType": room.get("roomType"),
            "checkIn": room.get("checkIn"),
            "checkOut": room.get("checkOut"),
        }

    return {}


def publish_booking_cancelled_event(email, booking_id, amount_spent):
    """
    Publish loyalty cancellation event to Redis stream used by loyalty consumer.
    """
    try:
        amount = int(float(amount_spent))
        points = amount // 10
        loyalty_event = {
            "event": "booking_cancelled",
            "email": str(email),
            "bookingId": str(booking_id),
            "amount": amount,
        }
        get_redis_client().xadd(redisStream, {"data": json.dumps(loyalty_event)})
        return {
            "event": "booking_cancelled",
            "email": str(email),
            "pointsDeducted": points,
            "publishError": None,
        }
    except Exception as exc:
        return {
            "event": "booking_cancelled",
            "email": str(email),
            "pointsDeducted": None,
            "publishError": str(exc),
        }


def enqueue_cancellation_email(email, booking_id, room_type, check_in, check_out):
    """
    Queue cancellation email payload for notification-wrapper consumer.
    """
    try:
        get_redis_client().xadd(
            notificationStream,
            {
                "to": str(email),
                "subject": f"Booking #{booking_id} cancelled",
                "text": (
                    f"Your booking #{booking_id} for {room_type} from {check_in} to {check_out} "
                    "has been cancelled and refund processing has been started."
                ),
            },
        )
        return {"queued": True, "error": None}
    except Exception as exc:
        return {"queued": False, "error": str(exc)}


# HEALTH CHECK
@app.get("/health")
def health():
    """
    Simple health probe for the orchestrator service.
    """
    return jsonify({"status": "ok", "service": "orchestrator"}), 200



# AUTH ENDPOINTS
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



# SCENARIO 1: BOOK & CONFIRM FLOW (Step 1 & 2 of booking)
@app.get("/route-rooms/options")
def routeRoomsOptions():
    """
    Return room type options and indicative costs for conversational clients.
    """
    try:
        upstream = requests.get(
            f"{roomsService}/rooms",
            timeout=forwardTimeout,
        )
    except requests.Timeout:
        return jsonify({"error": "Rooms service timeout"}), 504
    except requests.ConnectionError:
        return jsonify({"error": "Rooms service unavailable"}), 502

    try:
        rooms_data = upstream.json()
    except ValueError:
        return jsonify({"error": "Invalid response from rooms service"}), 502

    if upstream.status_code >= 400:
        return jsonify(rooms_data), upstream.status_code

    if not isinstance(rooms_data, list):
        return jsonify({"error": "Unexpected room options payload"}), 502

    def is_effectively_available(room):
        status = str(room.get("status", "")).lower()
        if status == "available":
            return True
        if status != "held":
            return False

        hold_expiry_raw = room.get("holdExpiry")
        if not hold_expiry_raw:
            return False

        try:
            expiry = parsedate_to_datetime(str(hold_expiry_raw))
            if expiry.tzinfo is None:
                expiry = expiry.replace(tzinfo=timezone.utc)
            return expiry <= datetime.now(timezone.utc)
        except Exception:
            return False

    grouped = {}
    for room in rooms_data:
        room_type = room.get("roomType")
        if not room_type:
            continue

        if room_type not in grouped:
            grouped[room_type] = {
                "roomType": room_type,
                "startingCost": None,
                "totalRooms": 0,
                "availableRooms": 0,
            }

        grouped[room_type]["totalRooms"] += 1
        if is_effectively_available(room):
            grouped[room_type]["availableRooms"] += 1

        raw_cost = room.get("costForTonight")
        try:
            cost = float(raw_cost)
            current = grouped[room_type]["startingCost"]
            if current is None or cost < current:
                grouped[room_type]["startingCost"] = cost
        except (TypeError, ValueError):
            continue

    options = sorted(grouped.values(), key=lambda item: item["roomType"])

    return jsonify({
        "currency": "SGD",
        "roomOptions": options,
    }), 200


# Scenario 1: Step 1 - Create room hold and payment intent
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

    pendingCardBookings[str(hold_id)] = {
        "holdId": hold_id,
        "roomID": hold_data.get("roomID"),
        "roomType": hold_data.get("roomType") or roomType,
        "checkIn": hold_data.get("checkIn") or checkIn,
        "checkOut": hold_data.get("checkOut") or checkOut,
        "amount": amount,
        "customerEmail": customerEmail,
        "customerMobile": customerMobile,
    }

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


@app.post("/route-rooms/book/card")
def routeRoomsBookCard():
    """
    Create and finalize a booking in one call using card payment.
    This endpoint is intended for card-based flows (e.g. scenarios 2/3 setup).
    """
    unauthorized = checkAuth()
    if unauthorized:
        return unauthorized

    payload = request.get_json(silent=True) or {}
    room_type = payload.get("roomType")
    check_in = payload.get("checkIn")
    check_out = payload.get("checkOut")
    customer_email = payload.get("customerEmail")
    customer_mobile = payload.get("customerMobile")
    card_number = payload.get("card_number")

    if not room_type or not check_in or not check_out or not customer_email or not customer_mobile:
        return jsonify({
            "error": "roomType, checkIn, checkOut, customerEmail and customerMobile are required"
        }), 400

    if not card_number:
        return jsonify({"error": "card_number is required"}), 400

    hold_payload = {
        "roomType": room_type,
        "checkIn": check_in,
        "checkOut": check_out,
        "customerEmail": customer_email,
        "customerMobile": customer_mobile,
    }
    hold_status, hold_data = request_json(
        "POST",
        f"{roomsService}/rooms/holds",
        hold_payload,
    )
    if hold_status >= 400:
        return jsonify({
            "error": "Failed to create room hold",
            "rooms": {"statusCode": hold_status, "data": hold_data},
        }), hold_status

    hold_id = hold_data.get("holdId")
    amount = hold_data.get("amount")
    if not hold_id or amount is None:
        return jsonify({"error": "rooms response missing holdId or amount"}), 502

    card_settle_payload = {
        "holdId": hold_id,
        "amount": amount,
        "card_number": card_number,
    }
    card_status, card_data = request_json(
        "POST",
        f"{paymentService}/payments/settle-card",
        card_settle_payload,
    )
    if card_status >= 400:
        return jsonify({
            "error": "Card settlement failed",
            "hold": hold_data,
            "payment": {"statusCode": card_status, "data": card_data},
        }), 502

    if (card_data.get("status") or "").upper() != "PAID":
        return jsonify({
            "error": "Card payment not marked as PAID",
            "payment": card_data,
        }), 400

    provided_booking_id = payload.get("bookingId")
    if provided_booking_id is not None:
        try:
            booking_id = int(provided_booking_id)
        except (TypeError, ValueError):
            return jsonify({"error": "bookingId must be an integer"}), 400
    else:
        booking_id = int(time.time() * 1000) % 2147483647

    booking_payload = {
        "id": booking_id,
        "roomID": hold_data.get("roomID"),
        "roomType": hold_data.get("roomType") or room_type,
        "customerEmail": customer_email,
        "customerMobile": customer_mobile,
        "checkIn": hold_data.get("checkIn") or check_in,
        "checkOut": hold_data.get("checkOut") or check_out,
        "amountSpent": amount,
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
            "error": "Missing booking data for card booking",
            "missingFields": missing_booking_fields,
            "hold": hold_data,
        }), 400

    room_update_payload = {
        "holdId": hold_id,
        "status": "reserved",
        "bookingId": booking_id,
        "reservationDate": payload.get("reservationDate"),
    }

    with ThreadPoolExecutor(max_workers=2) as executor:
        bookings_future = executor.submit(
            request_json,
            "POST",
            f"{bookingsService}/bookings/create",
            booking_payload,
        )
        rooms_future = executor.submit(
            request_json,
            "POST",
            f"{roomsService}/rooms/update",
            room_update_payload,
        )

        bookings_status, bookings_data = bookings_future.result()
        rooms_status, rooms_data = rooms_future.result()

    if bookings_status >= 400 or rooms_status >= 400:
        return jsonify({
            "error": "Card booking finalize failed",
            "payment": card_data,
            "bookings": {"statusCode": bookings_status, "data": bookings_data},
            "rooms": {"statusCode": rooms_status, "data": rooms_data},
        }), 502

    loyalty_publish_error = None
    try:
        amount_for_loyalty = int(float(booking_payload["amountSpent"]))
        points = amount_for_loyalty // 10
        loyalty_event = {
            "event": "booking_paid",
            "email": booking_payload["customerEmail"],
            "bookingId": str(booking_id),
            "amount": amount_for_loyalty,
        }
        get_redis_client().xadd(redisStream, {"data": json.dumps(loyalty_event)})
    except Exception as exc:
        points = None
        loyalty_publish_error = str(exc)

    return jsonify({
        "message": "Card booking finalized successfully",
        "payment": card_data,
        "booking": bookings_data,
        "room": rooms_data,
        "loyalty": {
            "event": "booking_paid",
            "email": booking_payload["customerEmail"],
            "points": points,
            "publishError": loyalty_publish_error,
        },
    }), 200


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

    loyalty_publish_error = None
    try:
        amount_for_loyalty = int(float(booking_payload["amountSpent"]))
        points = amount_for_loyalty // 10
        loyalty_event = {
            "event": "booking_paid",
            "email": booking_payload["customerEmail"],
            "bookingId": str(booking_id),
            "amount": amount_for_loyalty,
        }
        get_redis_client().xadd(
            redisStream,
            {"data": json.dumps(loyalty_event)},
        )
    except Exception as exc:
        points = None
        loyalty_publish_error = str(exc)

    pendingBookings.pop(intent_id, None)

    return jsonify({
        "message": "Booking finalized successfully",
        "settle": settle_data,
        "booking": bookings_data,
        "room": rooms_data,
        "loyalty": {
            "event": "booking_paid",
            "email": booking_payload["customerEmail"],
            "points": points,
            "publishError": loyalty_publish_error,
        },
    }), 200



# SCENARIO 2: CANCELLATION FLOW
@app.post("/route-bookings/bookings/<int:booking_id>/cancel")
def routeBookingsCancel(booking_id: int):
    """
    Cancel a booking by orchestrating refund, room release, booking delete,
    notification queueing, and loyalty points deduction event.
    """
    unauthorized = checkAuth()
    if unauthorized:
        return unauthorized

    payload = request.get_json(silent=True) or {}
    reason = str(payload.get("reason") or "BOOKING_CANCELLED")
    user = getUser() or {}
    user_identifier = (user.get("username") or "").strip().lower()
    user_role = str(user.get("role") or "").strip().lower()

    booking_status, booking_data = request_json(
        "GET",
        f"{bookingsService}/bookings/{booking_id}",
    )
    if booking_status >= 400:
        return jsonify({
            "error": "Failed to retrieve booking before cancellation",
            "bookings": {"statusCode": booking_status, "data": booking_data},
        }), booking_status

    if not isinstance(booking_data, dict):
        return jsonify({"error": "Unexpected booking payload from bookings service"}), 502

    hold_id = booking_data.get("hold_id") or booking_data.get("holdId")
    amount_spent = booking_data.get("amount_spent") or booking_data.get("amountSpent")
    customer_email = booking_data.get("customer_email") or booking_data.get("customerEmail")
    room_type = booking_data.get("room_type") or booking_data.get("roomType") or "room"
    check_in = booking_data.get("check_in") or booking_data.get("checkIn") or "N/A"
    check_out = booking_data.get("check_out") or booking_data.get("checkOut") or "N/A"

    missing_fields = [
        key
        for key, value in {
            "holdId": hold_id,
            "amountSpent": amount_spent,
            "customerEmail": customer_email,
        }.items()
        if value in (None, "")
    ]
    if missing_fields:
        return jsonify({
            "error": "Booking record is missing required cancellation fields",
            "missingFields": missing_fields,
            "booking": booking_data,
        }), 502

    booking_owner = str(customer_email).strip().lower()
    if user_role != "admin" and booking_owner != user_identifier:
        return jsonify({
            "error": "Forbidden: booking does not belong to the authenticated user",
            "bookingId": booking_id,
            "bookingOwner": customer_email,
            "authenticatedUser": user.get("username"),
        }), 403

    refund_payload = {
        "holdId": hold_id,
        "amount": amount_spent,
        "reason": reason,
    }
    room_update_payload = {
        "holdId": hold_id,
        "status": "available",
    }

    with ThreadPoolExecutor(max_workers=2) as executor:
        refund_future = executor.submit(
            request_json,
            "POST",
            f"{paymentService}/payments/refund",
            refund_payload,
        )
        rooms_future = executor.submit(
            request_json,
            "POST",
            f"{roomsService}/rooms/update",
            room_update_payload,
        )

        refund_status, refund_data = refund_future.result()
        rooms_status, rooms_data = rooms_future.result()

    if refund_status >= 400 or rooms_status >= 400:
        return jsonify({
            "error": "Cancellation failed during refund or room update",
            "booking": booking_data,
            "refund": {"statusCode": refund_status, "data": refund_data},
            "rooms": {"statusCode": rooms_status, "data": rooms_data},
        }), 502

    delete_status, delete_data = request_json(
        "DELETE",
        f"{bookingsService}/bookings/{booking_id}",
    )
    if delete_status >= 400:
        return jsonify({
            "error": "Booking cancellation partially completed; booking delete failed",
            "booking": booking_data,
            "refund": {"statusCode": refund_status, "data": refund_data},
            "rooms": {"statusCode": rooms_status, "data": rooms_data},
            "bookingsDelete": {"statusCode": delete_status, "data": delete_data},
        }), 502

    with ThreadPoolExecutor(max_workers=2) as executor:
        notification_future = executor.submit(
            enqueue_cancellation_email,
            customer_email,
            booking_id,
            room_type,
            check_in,
            check_out,
        )
        loyalty_future = executor.submit(
            publish_booking_cancelled_event,
            customer_email,
            booking_id,
            amount_spent,
        )

        notification_result = notification_future.result()
        loyalty_result = loyalty_future.result()

    return jsonify({
        "message": "Booking cancelled successfully",
        "booking": booking_data,
        "refund": refund_data,
        "room": rooms_data,
        "bookingsDelete": delete_data,
        "notification": notification_result,
        "loyalty": loyalty_result,
    }), 200


# ═══════════════════════════════════════════════════════════════════════════════════
# SCENARIO 3: BOOKING MODIFICATION FLOW (Step 1 & 2: Preview → Confirm)
# ═══════════════════════════════════════════════════════════════════════════════════

@app.post("/route-rooms/modify")
def routeRoomsModifyPreview():
    """
    Step 1: Preview booking modification with new hold and price difference.
    
    Input: old_booking_id, new_room_type, new_check_in, new_check_out
    Output: old_cost, new_cost, difference, new_hold_id, old_hold_id (for confirm step)
    """
    unauthorized = checkAuth()
    if unauthorized:
        return unauthorized

    payload = request.get_json(silent=True) or {}
    old_booking_id = payload.get("old_booking_id")
    new_room_type = payload.get("new_room_type")
    new_check_in = payload.get("new_check_in")
    new_check_out = payload.get("new_check_out")
    user = getUser() or {}
    user_identifier = (user.get("username") or "").strip().lower()
    user_role = str(user.get("role") or "").strip().lower()

    if not old_booking_id or not new_room_type or not new_check_in or not new_check_out:
        return jsonify({
            "error": "old_booking_id, new_room_type, new_check_in, new_check_out are required"
        }), 400

    booking_status, booking_data = request_json(
        "GET",
        f"{bookingsService}/bookings/{old_booking_id}",
    )
    if booking_status >= 400:
        return jsonify({
            "error": "Failed to retrieve old booking",
            "bookings": {"statusCode": booking_status, "data": booking_data},
        }), booking_status

    if not isinstance(booking_data, dict):
        return jsonify({"error": "Unexpected booking payload"}), 502

    old_hold_id = booking_data.get("hold_id") or booking_data.get("holdId")
    old_amount_spent = booking_data.get("amount_spent") or booking_data.get("amountSpent")
    customer_email = booking_data.get("customer_email") or booking_data.get("customerEmail")
    old_room_type = booking_data.get("room_type") or booking_data.get("roomType")

    if not old_hold_id or old_amount_spent is None or not customer_email:
        return jsonify({
            "error": "Old booking missing required fields for modification",
            "missingFields": [k for k in ["holdId", "amountSpent", "customerEmail"] 
                            if booking_data.get(k) is None]
        }), 502

    booking_owner = str(customer_email).strip().lower()
    if user_role != "admin" and booking_owner != user_identifier:
        return jsonify({
            "error": "Forbidden: booking does not belong to the authenticated user",
            "bookingId": old_booking_id,
            "bookingOwner": customer_email,
            "authenticatedUser": user.get("username"),
        }), 403

    new_hold_payload = {
        "roomType": new_room_type,
        "checkIn": new_check_in,
        "checkOut": new_check_out,
        "customerEmail": customer_email,
        "customerMobile": booking_data.get("customer_mobile") or booking_data.get("customerMobile"),
    }

    new_hold_status, new_hold_data = request_json(
        "POST",
        f"{roomsService}/rooms/holds",
        new_hold_payload,
    )
    if new_hold_status >= 400:
        return jsonify({
            "error": "Failed to create new hold for proposed modification",
            "rooms": {"statusCode": new_hold_status, "data": new_hold_data},
        }), new_hold_status

    new_hold_id = new_hold_data.get("holdId")
    new_amount = new_hold_data.get("amount")

    if not new_hold_id or new_amount is None:
        return jsonify({
            "error": "New hold response missing required fields"
        }), 502

    old_amount = int(float(old_amount_spent))
    new_amount_int = int(float(new_amount))
    difference = new_amount_int - old_amount
    loyalty_delta = difference // 10

    return jsonify({
        "message": "Modification preview generated successfully",
        "old_booking": {
            "bookingId": old_booking_id,
            "roomType": old_room_type,
            "cost": old_amount,
            "holdId": old_hold_id,
        },
        "new_booking": {
            "roomType": new_room_type,
            "cost": new_amount_int,
            "holdId": new_hold_id,
            "checkIn": new_check_in,
            "checkOut": new_check_out,
        },
        "price_adjustment": {
            "old_cost": old_amount,
            "new_cost": new_amount_int,
            "difference": difference,
            "action": "charge" if difference > 0 else ("refund" if difference < 0 else "none"),
            "loyalty_delta_points": loyalty_delta,
        },
    }), 200


@app.post("/route-rooms/book/modify-confirm")
def routeRoomsConfirmModification():
    """
    Step 2: Confirm booking modification with payment settlement and room updates.
    
        Input: old_booking_id, new_room_type, new_check_in, new_check_out,
            new_amount_spent, new_booking_id (optional), new_hold_id,
            old_hold_id (optional, validated if provided),
            card_number (required only if difference > 0)
    """
    unauthorized = checkAuth()
    if unauthorized:
        return unauthorized

    payload = request.get_json(silent=True) or {}
    old_booking_id = payload.get("old_booking_id")
    new_booking_id = payload.get("new_booking_id")
    old_hold_id_input = payload.get("old_hold_id")
    new_hold_id = payload.get("new_hold_id")
    card_number = payload.get("card_number")
    user = getUser() or {}
    user_identifier = (user.get("username") or "").strip().lower()
    user_role = str(user.get("role") or "").strip().lower()

    if not old_booking_id or not new_hold_id:
        return jsonify({
            "error": "old_booking_id and new_hold_id are required"
        }), 400

    booking_status, booking_data = request_json(
        "GET",
        f"{bookingsService}/bookings/{old_booking_id}",
    )
    if booking_status >= 400:
        return jsonify({
            "error": "Failed to retrieve old booking",
            "bookings": {"statusCode": booking_status, "data": booking_data},
        }), booking_status

    if not isinstance(booking_data, dict):
        return jsonify({"error": "Unexpected booking payload"}), 502

    customer_email = booking_data.get("customer_email") or booking_data.get("customerEmail")
    old_amount_spent = booking_data.get("amount_spent") or booking_data.get("amountSpent")
    old_hold_id = booking_data.get("hold_id") or booking_data.get("holdId")

    if not customer_email or old_amount_spent is None or not old_hold_id:
        return jsonify({
            "error": "Old booking missing required fields for modification",
            "booking": booking_data,
        }), 502

    if old_hold_id_input and str(old_hold_id_input) != str(old_hold_id):
        return jsonify({
            "error": "old_hold_id does not match booking record",
            "providedOldHoldId": old_hold_id_input,
            "expectedOldHoldId": old_hold_id,
        }), 400

    booking_owner = str(customer_email).strip().lower()
    if user_role != "admin" and booking_owner != user_identifier:
        return jsonify({
            "error": "Forbidden: booking does not belong to the authenticated user",
            "bookingId": old_booking_id,
        }), 403

    new_amount_spent = payload.get("new_amount_spent")
    if new_amount_spent is None:
        return jsonify({
            "error": "new_amount_spent is required. Use /route-rooms/modify response value."
        }), 400

    try:
        new_amount_spent = int(float(new_amount_spent))
    except (TypeError, ValueError):
        return jsonify({"error": "new_amount_spent must be numeric"}), 400

    if new_amount_spent <= 0:
        return jsonify({"error": "new_amount_spent must be positive"}), 400

    old_amount = int(float(old_amount_spent))
    difference = new_amount_spent - old_amount
    loyalty_delta = difference // 10

    settle_payment_status = None
    settle_payment_data = None
    refund_status = None
    refund_data = None

    if difference > 0:
        if not card_number:
            return jsonify({
                "error": "card_number required for additional payment",
                "difference": difference,
            }), 400

        settle_payload = {
            "holdId": new_hold_id,
            "amount": difference,
            "card_number": card_number,
        }
        settle_payment_status, settle_payment_data = request_json(
            "POST",
            f"{paymentService}/payments/settle-card",
            settle_payload,
        )
        if settle_payment_status >= 400:
            return jsonify({
                "error": "Payment settlement failed",
                "payment": {"statusCode": settle_payment_status, "data": settle_payment_data},
            }), 502

        if (settle_payment_data.get("status") or "").upper() != "PAID":
            return jsonify({
                "error": "Payment not marked as PAID",
                "payment": settle_payment_data,
            }), 400

    elif difference < 0:
        refund_payload = {
            "holdId": old_hold_id,
            "amount": abs(difference),
            "reason": "BOOKING_MODIFIED",
        }
        refund_status, refund_data = request_json(
            "POST",
            f"{paymentService}/payments/refund",
            refund_payload,
        )
        if refund_status >= 400:
            return jsonify({
                "error": "Refund issuance failed",
                "refund": {"statusCode": refund_status, "data": refund_data},
            }), 502

    if not new_booking_id:
        new_booking_id = int(time.time() * 1000) % 2147483647

    new_booking_payload = {
        "id": new_booking_id,
        "roomID": payload.get("roomID") or booking_data.get("room_id") or booking_data.get("roomID"),
        "roomType": payload.get("new_room_type") or booking_data.get("room_type") or booking_data.get("roomType"),
        "customerEmail": customer_email,
        "customerMobile": booking_data.get("customer_mobile") or booking_data.get("customerMobile"),
        "checkIn": payload.get("new_check_in"),
        "checkOut": payload.get("new_check_out"),
        "amountSpent": new_amount_spent,
        "holdId": new_hold_id,
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
        ] if new_booking_payload.get(key) in (None, "")
    ]
    if missing_booking_fields:
        return jsonify({
            "error": "Missing required booking fields for modification confirm",
            "missingFields": missing_booking_fields,
        }), 400

    room_release_payload = {
        "holdId": old_hold_id,
        "status": "available",
    }

    room_confirm_payload = {
        "holdId": new_hold_id,
        "status": "reserved",
        "bookingId": new_booking_id,
    }

    with ThreadPoolExecutor(max_workers=3) as executor:
        new_booking_future = executor.submit(
            request_json,
            "POST",
            f"{bookingsService}/bookings/create",
            new_booking_payload,
        )
        room_release_future = executor.submit(
            request_json,
            "POST",
            f"{roomsService}/rooms/update",
            room_release_payload,
        )
        room_confirm_future = executor.submit(
            request_json,
            "POST",
            f"{roomsService}/rooms/update",
            room_confirm_payload,
        )

        new_booking_status, new_booking_data = new_booking_future.result()
        room_release_status, room_release_data = room_release_future.result()
        room_confirm_status, room_confirm_data = room_confirm_future.result()

    if new_booking_status >= 400 or room_release_status >= 400 or room_confirm_status >= 400:
        return jsonify({
            "error": "Modification failed during booking/room updates",
            "new_booking": {"statusCode": new_booking_status, "data": new_booking_data},
            "room_release": {"statusCode": room_release_status, "data": room_release_data},
            "room_confirm": {"statusCode": room_confirm_status, "data": room_confirm_data},
        }), 502

    delete_old_booking_status, delete_old_booking_data = request_json(
        "DELETE",
        f"{bookingsService}/bookings/{old_booking_id}",
    )
    if delete_old_booking_status >= 400:
        return jsonify({
            "error": "Old booking deletion failed after modification",
            "new_booking": {"statusCode": new_booking_status, "data": new_booking_data},
            "delete_old": {"statusCode": delete_old_booking_status, "data": delete_old_booking_data},
        }), 502

    loyalty_event = {
        "event": "booking_modified",
        "email": customer_email,
        "bookingId": str(new_booking_id),
        "old_amount": old_amount,
        "new_amount": new_amount_spent,
    }
    
    loyalty_publish_error = None
    try:
        get_redis_client().xadd(
            redisStream,
            {"data": json.dumps(loyalty_event)},
        )
    except Exception as exc:
        loyalty_publish_error = str(exc)

    return jsonify({
        "message": "Booking modified successfully",
        "old_booking": {
            "bookingId": old_booking_id,
            "cost": old_amount,
        },
        "new_booking": new_booking_data,
        "payment_settlement": {
            "difference": difference,
            "action": "charge" if difference > 0 else ("refund" if difference < 0 else "none"),
            "payment": settle_payment_data if difference > 0 else None,
            "refund": refund_data if difference < 0 else None,
        },
        "rooms": {
            "old_released": room_release_data,
            "new_confirmed": room_confirm_data,
        },
        "loyalty": {
            "event": "booking_modified",
            "email": customer_email,
            "old_amount": old_amount,
            "new_amount": new_amount_spent,
            "points_delta": loyalty_delta,
            "publishError": loyalty_publish_error,
        },
    }), 200


# ═══════════════════════════════════════════════════════════════════════════════════
# SUPPORT & UTILITY ENDPOINTS (Used by scenarios, safe to keep)
# ═══════════════════════════════════════════════════════════════════════════════════

# Helper endpoints to route to bookings service for booking retrieval, creation, update and deletion.
@app.get("/route-bookings/bookings/<int:booking_id>")
def routeBookingsGet(booking_id: int):
    """
    Return booking details for the given ID from the bookings service.
    """
    unauthorized = checkAuth()
    if unauthorized:
        return unauthorized
    return forward("GET", f"{bookingsService}/bookings/{booking_id}")


@app.get("/route-bookings/my-bookings")
def routeBookingsMine():
    """
    Return bookings that belong to the currently authenticated user.
    """
    unauthorized = checkAuth()
    if unauthorized:
        return unauthorized

    user = getUser() or {}
    user_identifier = (user.get("email") or user.get("username") or "").strip()
    if not user_identifier:
        return jsonify({"error": "Authenticated user missing email/username"}), 400

    status_code, data = request_json(
        "GET",
        f"{bookingsService}/bookings",
        params={"customerEmail": user_identifier},
    )
    return jsonify(data), status_code


# These endpoints bypass orchestration and are intended for testing only.
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
    Settle a card payment and auto-finalize booking when hold context is available.
    """
    payload = request.get_json(silent=True) or {}
    hold_id = payload.get("holdId")
    amount = payload.get("amount")
    card_number = payload.get("card_number")
    customer_email = str(payload.get("customerEmail") or "").strip()
    customer_mobile = str(payload.get("customerMobile") or "").strip()

    if not hold_id:
        return jsonify({"error": "holdId is required"}), 400
    if amount is None:
        return jsonify({"error": "amount is required"}), 400
    if not card_number:
        return jsonify({"error": "card_number is required"}), 400
    if not customer_email:
        return jsonify({"error": "customerEmail is required"}), 400
    if not customer_mobile:
        return jsonify({"error": "customerMobile is required"}), 400

    context = pendingCardBookings.get(str(hold_id), {}).copy()
    if not context:
        context = get_hold_context_from_rooms(hold_id)

    settle_amount = amount
    context_amount = context.get("amount")
    if context_amount is not None:
        try:
            requested_amount = int(float(amount))
            expected_amount = int(float(context_amount))
        except (TypeError, ValueError):
            return jsonify({"error": "amount must be numeric"}), 400

        if requested_amount != expected_amount:
            return jsonify({
                "error": "amount does not match hold amount",
                "requestedAmount": requested_amount,
                "expectedAmount": expected_amount,
                "holdId": hold_id,
            }), 400

        settle_amount = expected_amount

    settle_status, settle_data = request_json(
        "POST",
        f"{paymentService}/payments/settle-card",
        {
            "holdId": hold_id,
            "amount": settle_amount,
            "card_number": card_number,
        },
    )
    if settle_status >= 400:
        return jsonify(settle_data), settle_status

    if (settle_data.get("status") or "").upper() != "PAID":
        return jsonify({
            "error": "Card payment not marked as PAID",
            "payment": settle_data,
        }), 400

    provided_booking_id = payload.get("bookingId")
    if provided_booking_id is not None:
        try:
            booking_id = int(provided_booking_id)
        except (TypeError, ValueError):
            return jsonify({"error": "bookingId must be an integer"}), 400
    else:
        booking_id = int(time.time() * 1000) % 2147483647

    booking_payload = {
        "id": booking_id,
        "roomID": payload.get("roomID") or context.get("roomID"),
        "roomType": payload.get("roomType") or context.get("roomType"),
        "customerEmail": customer_email,
        "customerMobile": customer_mobile,
        "checkIn": payload.get("checkIn") or context.get("checkIn"),
        "checkOut": payload.get("checkOut") or context.get("checkOut"),
        "amountSpent": payload.get("amountSpent") or context.get("amount") or amount,
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
            "message": "Payment settled, but booking was not auto-created due to missing fields",
            "payment": settle_data,
            "autoBookingCreated": False,
            "missingFields": missing_booking_fields,
            "hint": "Pass complete booking fields in settle-card request (customerEmail/customerMobile are now mandatory)",
        }), 202

    room_update_payload = {
        "holdId": hold_id,
        "status": payload.get("roomStatus", "reserved"),
        "bookingId": booking_id,
        "reservationDate": payload.get("reservationDate"),
    }

    with ThreadPoolExecutor(max_workers=2) as executor:
        bookings_future = executor.submit(
            request_json,
            "POST",
            f"{bookingsService}/bookings/create",
            booking_payload,
        )
        rooms_future = executor.submit(
            request_json,
            "POST",
            f"{roomsService}/rooms/update",
            room_update_payload,
        )

        bookings_status, bookings_data = bookings_future.result()
        rooms_status, rooms_data = rooms_future.result()

    if bookings_status >= 400 or rooms_status >= 400:
        return jsonify({
            "error": "Card settlement succeeded but booking finalization failed",
            "payment": settle_data,
            "bookings": {"statusCode": bookings_status, "data": bookings_data},
            "rooms": {"statusCode": rooms_status, "data": rooms_data},
        }), 502

    loyalty_publish_error = None
    try:
        amount_for_loyalty = int(float(booking_payload["amountSpent"]))
        points = amount_for_loyalty // 10
        loyalty_event = {
            "event": "booking_paid",
            "email": booking_payload["customerEmail"],
            "bookingId": str(booking_id),
            "amount": amount_for_loyalty,
        }
        get_redis_client().xadd(redisStream, {"data": json.dumps(loyalty_event)})
    except Exception as exc:
        points = None
        loyalty_publish_error = str(exc)

    pendingCardBookings.pop(str(hold_id), None)

    return jsonify({
        "message": "Card settled and booking finalized successfully",
        "payment": settle_data,
        "booking": bookings_data,
        "room": rooms_data,
        "loyalty": {
            "event": "booking_paid",
            "email": booking_payload["customerEmail"],
            "points": points,
            "publishError": loyalty_publish_error,
        },
        "autoBookingCreated": True,
    }), 200


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


@app.post("/route-bookings/bookings/create")
def routeBookingsCreate():
    """
    Create a new booking record through the bookings service.
    """
    unauthorized = checkAuth()
    if unauthorized:
        return unauthorized
    return forward("POST", f"{bookingsService}/bookings/create")


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

    hold_status, hold_data = request_json("POST", f"{roomsService}/rooms/holds", payload)

    if hold_status < 400 and isinstance(hold_data, dict):
        hold_id = hold_data.get("holdId")
        amount = hold_data.get("amount")
        if hold_id and amount is not None:
            pendingCardBookings[str(hold_id)] = {
                "holdId": hold_id,
                "roomID": hold_data.get("roomID"),
                "roomType": hold_data.get("roomType") or roomType,
                "checkIn": hold_data.get("checkIn") or checkIn,
                "checkOut": hold_data.get("checkOut") or checkOut,
                "amount": amount,
                "customerEmail": payload.get("customerEmail"),
                "customerMobile": payload.get("customerMobile"),
            }

    return jsonify(hold_data), hold_status


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

    get_redis_client().xadd(
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

    get_redis_client().xadd(
        redisStream,
        {
            "event": "deduct_points",
            "email": email,
            "points": str(points),
        },
    )

    return jsonify({"status": "queued", "event": "deduct_points"}), 202

# Add flask in built threading to handle multiple requests, no point for kong imo with added latency
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, threaded=True)
