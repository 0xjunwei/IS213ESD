import os
import time
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import redis
import requests
from flask import Flask, jsonify, request
from flask_cors import CORS

app = Flask(__name__)
CORS(app)


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

# Booking context keyed by payment intent id.
pendingBookings = {}

# Hold context for card-settle flow keyed by holdId.
pendingCardBookings = {}


# Redis client (used for loyalty + notification events)
redis_client = None


def getRedisClient():
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


# Helpers
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

def computeRefundSplit(holdIds, totalAmount):
    """Build a refund split across hold IDs using payment-service capacity."""
    capacities = []
    warnings = []

    for holdId in holdIds:
        status, data = requestJson(
            "GET",
            f"{paymentService}/payments/refund-capacity/{holdId}",
        )

        if status == 200 and isinstance(data, dict):
            try:
                remaining = int(float(data.get("remainingRefundable", 0)))
            except (TypeError, ValueError):
                remaining = 0

            capacities.append({
                "holdId": holdId,
                "remaining": max(remaining, 0),
                "raw": data,
            })
            continue

        if status == 404:
            warnings.append(f"No paid payment found for holdId {holdId}")
            capacities.append({"holdId": holdId, "remaining": 0, "raw": data})
            continue

        return None, {
            "error": "Failed to fetch refund capacity",
            "holdId": holdId,
            "payment": {"statusCode": status, "data": data},
        }, warnings

    remainingTarget = int(totalAmount)
    split = []

    for item in capacities:
        if remainingTarget <= 0:
            break

        refundableHere = min(item["remaining"], remainingTarget)
        if refundableHere > 0:
            split.append({
                "holdId": item["holdId"],
                "amount": refundableHere,
            })
            remainingTarget -= refundableHere

    return {
        "split": split,
        "shortfall": remainingTarget,
        "capacities": capacities,
    }, None, warnings


def checkAuth():
    """
    Call this at the start of any route that needs auth.
    Return (response, status) if unauthorized, else None.
    """
    user = getUser()
    if not user:
        return jsonify({"error": "Unauthorized"}), 401
    return None


def normalizeDateString(value):
    """
    Normalize date string to YYYY-MM-DD format.
    Handles RFC format (e.g., 'Wed, 25 Mar 2026 00:00:00 GMT'),
    ISO format (e.g., '2026-03-25T00:00:00Z'), and YYYY-MM-DD format.
    Returns None if unable to parse.
    """
    if not value:
        return None
    
    text = str(value).strip()
    if not text:
        return None
    
    # Already in YYYY-MM-DD format
    if len(text) == 10 and text[4] == '-' and text[7] == '-':
        try:
            datetime.strptime(text, "%Y-%m-%d")
            return text
        except ValueError:
            pass
    
    # Try parsing with various formats
    formats = [
        "%Y-%m-%d %H:%M:%S",           # 2026-03-25 00:00:00
        "%Y-%m-%dT%H:%M:%SZ",          # 2026-03-25T00:00:00Z
        "%Y-%m-%dT%H:%M:%S.%fZ",       # 2026-03-25T00:00:00.000Z
        "%a, %d %b %Y %H:%M:%S %Z",    # Wed, 25 Mar 2026 00:00:00 GMT (RFC 2822)
    ]
    
    for fmt in formats:
        try:
            dt = datetime.strptime(text.replace('GMT', 'UTC'), fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    
    # Final fallback: try general parsing
    try:
        dt = datetime.fromisoformat(text.replace('Z', '+00:00'))
        return dt.strftime("%Y-%m-%d")
    except (ValueError, AttributeError):
        pass
    
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


def requestJson(method, url, payload=None, params=None):
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


def getHoldContextFromRooms(holdId):
    """
    Resolve hold context from rooms service so settle-card can still finalize after restarts.
    """
    if not holdId:
        return {}

    status_code, rooms_data = requestJson("GET", f"{roomsService}/rooms")
    if status_code >= 400 or not isinstance(rooms_data, list):
        return {}

    holdIdStr = str(holdId)
    for room in rooms_data:
        if str(room.get("holdId") or "") != holdIdStr:
            continue

        return {
            "holdId": holdIdStr,
            "roomID": room.get("roomID"),
            "roomType": room.get("roomType"),
            "checkIn": room.get("checkIn"),
            "checkOut": room.get("checkOut"),
        }

    return {}


def publishBookingCancelledEvent(email, booking_id, amount_spent):
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
        getRedisClient().xadd(redisStream, {"data": json.dumps(loyalty_event)})
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


def enqueueCancellationEmail(email, booking_id, room_type, check_in, check_out):
    """
    Queue cancellation email payload for notification-wrapper consumer.
    """
    try:
        getRedisClient().xadd(
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



# Booking flow
@app.get("/route-rooms/options")
def routeRoomsOptions():
    """
    Return room type options and indicative costs.
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

    # Normalize date strings before calling rooms service.
    normalized_check_in = normalizeDateString(checkIn)
    normalized_check_out = normalizeDateString(checkOut)
    
    if not normalized_check_in or not normalized_check_out:
        return jsonify({
            "error": "Invalid date format for check-in or check-out",
            "received": {
                "checkIn": checkIn,
                "checkOut": checkOut,
            },
            "expected_format": "YYYY-MM-DD or RFC format (e.g., 'Wed, 25 Mar 2026 00:00:00 GMT')"
        }), 400

    normalized_payload = {
        "roomType": roomType,
        "checkIn": normalized_check_in,
        "checkOut": normalized_check_out,
        "customerEmail": customerEmail,
        "customerMobile": customerMobile,
    }

    try:
        hold_resp = requests.post(
            f"{roomsService}/rooms/holds",
            json=normalized_payload,
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
        return jsonify({
            "error": "Failed to create room hold",
            "rooms": hold_data,
            "sentPayload": normalized_payload,
        }), hold_resp.status_code

    hold_id = hold_data.get("holdId")
    amount = hold_data.get("amount")

    if not hold_id or amount is None:
        return jsonify({"error": "rooms response missing holdId or amount"}), 502

    pendingCardBookings[str(hold_id)] = {
        "holdId": hold_id,
        "roomID": hold_data.get("roomID"),
        "roomType": hold_data.get("roomType") or roomType,
        "checkIn": hold_data.get("checkIn") or normalized_check_in,
        "checkOut": hold_data.get("checkOut") or normalized_check_out,
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
                "checkIn": hold_data.get("checkIn") or normalized_check_in,
                "checkOut": hold_data.get("checkOut") or normalized_check_out,
                "amount": amount,
                "customerEmail": customerEmail,
                "customerMobile": customerMobile,
            }

    return jsonify(payment_data), payment_resp.status_code


@app.post("/route-rooms/book/card")
def routeRoomsBookCard():
    """
    Create and finalize a booking in one call using card payment.
    """
    unauthorized = checkAuth()
    if unauthorized:
        return unauthorized

    payload = request.get_json(silent=True) or {}
    roomType = payload.get("roomType")
    checkIn = payload.get("checkIn")
    checkOut = payload.get("checkOut")
    customerEmail = payload.get("customerEmail")
    customerMobile = payload.get("customerMobile")
    cardNumber = payload.get("card_number")

    if not roomType or not checkIn or not checkOut or not customerEmail or not customerMobile:
        return jsonify({
            "error": "roomType, checkIn, checkOut, customerEmail and customerMobile are required"
        }), 400

    if not cardNumber:
        return jsonify({"error": "card_number is required"}), 400

    # Normalize date strings before calling rooms service.
    normalizedCheckIn = normalizeDateString(checkIn)
    normalizedCheckOut = normalizeDateString(checkOut)
    
    if not normalizedCheckIn or not normalizedCheckOut:
        return jsonify({
            "error": "Invalid date format for check-in or check-out",
            "received": {
                "checkIn": checkIn,
                "checkOut": checkOut,
            },
            "expected_format": "YYYY-MM-DD or RFC format (e.g., 'Wed, 25 Mar 2026 00:00:00 GMT')"
        }), 400

    holdPayload = {
        "roomType": roomType,
        "checkIn": normalizedCheckIn,
        "checkOut": normalizedCheckOut,
        "customerEmail": customerEmail,
        "customerMobile": customerMobile,
    }
    holdStatus, holdData = requestJson(
        "POST",
        f"{roomsService}/rooms/holds",
        holdPayload,
    )
    if holdStatus >= 400:
        return jsonify({
            "error": "Failed to create room hold",
            "rooms": {"statusCode": holdStatus, "data": holdData},
            "sentPayload": holdPayload,
        }), holdStatus

    holdId = holdData.get("holdId")
    amount = holdData.get("amount")
    if not holdId or amount is None:
        return jsonify({"error": "rooms response missing holdId or amount"}), 502

    cardSettlePayload = {
        "holdId": holdId,
        "amount": amount,
        "card_number": cardNumber,
    }
    cardStatus, cardData = requestJson(
        "POST",
        f"{paymentService}/payments/settle-card",
        cardSettlePayload,
    )
    if cardStatus >= 400:
        return jsonify({
            "error": "Card settlement failed",
            "hold": holdData,
            "payment": {"statusCode": cardStatus, "data": cardData},
        }), 502

    if (cardData.get("status") or "").upper() != "PAID":
        return jsonify({
            "error": "Card payment not marked as PAID",
            "payment": cardData,
        }), 400

    providedBookingId = payload.get("bookingId")
    if providedBookingId is not None:
        try:
            bookingId = int(providedBookingId)
        except (TypeError, ValueError):
            return jsonify({"error": "bookingId must be an integer"}), 400
    else:
        bookingId = int(time.time() * 1000) % 2147483647

    bookingPayload = {
        "id": bookingId,
        "roomID": holdData.get("roomID"),
        "roomType": holdData.get("roomType") or roomType,
        "customerEmail": customerEmail,
        "customerMobile": customerMobile,
        "checkIn": holdData.get("checkIn") or checkIn,
        "checkOut": holdData.get("checkOut") or checkOut,
        "amountSpent": amount,
        "holdId": holdId,
    }

    missingBookingFields = [
        key for key in [
            "roomID",
            "roomType",
            "customerEmail",
            "customerMobile",
            "checkIn",
            "checkOut",
            "amountSpent",
            "holdId",
        ] if bookingPayload.get(key) in (None, "")
    ]
    if missingBookingFields:
        return jsonify({
            "error": "Missing booking data for card booking",
            "missingFields": missingBookingFields,
            "hold": holdData,
        }), 400

    roomUpdatePayload = {
        "holdId": holdId,
        "status": "reserved",
        "bookingId": bookingId,
        "reservationDate": payload.get("reservationDate"),
    }

    with ThreadPoolExecutor(max_workers=2) as executor:
        bookingsFuture = executor.submit(
            requestJson,
            "POST",
            f"{bookingsService}/bookings/create",
            bookingPayload,
        )
        roomsFuture = executor.submit(
            requestJson,
            "POST",
            f"{roomsService}/rooms/update",
            roomUpdatePayload,
        )

        bookingsStatus, bookingsData = bookingsFuture.result()
        roomsStatus, roomsData = roomsFuture.result()

    if bookingsStatus >= 400 or roomsStatus >= 400:
        return jsonify({
            "error": "Card booking finalize failed",
            "payment": cardData,
            "bookings": {"statusCode": bookingsStatus, "data": bookingsData},
            "rooms": {"statusCode": roomsStatus, "data": roomsData},
        }), 502

    loyaltyPublishError = None
    try:
        amountForLoyalty = int(float(bookingPayload["amountSpent"]))
        points = amountForLoyalty // 10
        loyaltyEvent = {
            "event": "booking_paid",
            "email": bookingPayload["customerEmail"],
            "bookingId": str(bookingId),
            "amount": amountForLoyalty,
        }
        getRedisClient().xadd(redisStream, {"data": json.dumps(loyaltyEvent)})
    except Exception as exc:
        points = None
        loyaltyPublishError = str(exc)

    return jsonify({
        "message": "Card booking finalized successfully",
        "payment": cardData,
        "booking": bookingsData,
        "room": roomsData,
        "loyalty": {
            "event": "booking_paid",
            "email": bookingPayload["customerEmail"],
            "points": points,
            "publishError": loyaltyPublishError,
        },
    }), 200


@app.post("/route-rooms/book/confirm")
def routeRoomsConfirmBooking():
    """
    Settle payment, then concurrently create booking and reserve room.
    """
    payload = request.get_json(silent=True) or {}

    intentId = payload.get("intentID") or payload.get("paymentIntentId")
    txHash = payload.get("TxnID_Proof") or payload.get("tx_hash")
    holdIdInput = payload.get("holdID") or payload.get("holdId")

    if not intentId or not txHash or not holdIdInput:
        return jsonify({"error": "intentID, TxnID_Proof and holdID are required"}), 400

    settlePayload = {
        "paymentIntentId": intentId,
        "tx_hash": txHash,
    }

    try:
        settleResp = requests.post(
            f"{paymentService}/payments/settle",
            json=settlePayload,
            headers={"Content-Type": "application/json"},
            timeout=forwardTimeout,
        )
    except requests.Timeout:
        return jsonify({"error": "Payment service timeout"}), 504
    except requests.ConnectionError:
        return jsonify({"error": "Payment service unavailable"}), 502

    try:
        settleData = settleResp.json()
    except ValueError:
        settleData = {"message": settleResp.text}

    if settleResp.status_code >= 400:
        return jsonify({"settle": settleData}), settleResp.status_code

    if (settleData.get("status") or "").upper() != "PAID":
        return jsonify({"settle": settleData, "error": "payment not marked as PAID"}), 400

    bookingContext = pendingBookings.get(intentId, {})

    holdId = holdIdInput
    cachedHoldId = bookingContext.get("holdId")
    if cachedHoldId and cachedHoldId != holdId:
        return jsonify({
            "error": "holdID does not match booking context for this paymentIntentId",
            "expectedHoldId": cachedHoldId,
            "providedHoldId": holdId,
        }), 400

    amountSpent = bookingContext.get("amount") or payload.get("amount")
    providedBookingId = payload.get("bookingId")
    if providedBookingId is not None:
        try:
            bookingId = int(providedBookingId)
        except (TypeError, ValueError):
            return jsonify({"error": "bookingId must be an integer"}), 400
    else:
        # Keep id inside signed INT range used by downstream MySQL schemas.
        bookingId = int(time.time() * 1000) % 2147483647

    bookingPayload = {
        "id": bookingId,
        "roomID": bookingContext.get("roomID") or payload.get("roomID"),
        "roomType": bookingContext.get("roomType") or payload.get("roomType"),
        "customerEmail": bookingContext.get("customerEmail") or payload.get("customerEmail"),
        "customerMobile": bookingContext.get("customerMobile") or payload.get("customerMobile"),
        "checkIn": bookingContext.get("checkIn") or payload.get("checkIn"),
        "checkOut": bookingContext.get("checkOut") or payload.get("checkOut"),
        "amountSpent": amountSpent,
        "holdId": holdId,
    }

    missingBookingFields = [
        key for key in [
            "roomID",
            "roomType",
            "customerEmail",
            "customerMobile",
            "checkIn",
            "checkOut",
            "amountSpent",
            "holdId",
        ] if bookingPayload.get(key) in (None, "")
    ]
    if missingBookingFields:
        return jsonify({
            "error": "Missing booking data for finalize step",
            "missingFields": missingBookingFields,
            "hint": "Pass the fields in /route-rooms/book or include them in this confirm request",
        }), 400

    roomUpdatePayload = {
        "holdId": holdId,
        "status": payload.get("roomStatus", "reserved"),
        "bookingId": bookingId,
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
                bodyData = response.json()
            except ValueError:
                bodyData = {"message": response.text}
            return response.status_code, bodyData
        except requests.Timeout:
            return 504, {"error": "Upstream timeout"}
        except requests.ConnectionError:
            return 502, {"error": "Upstream unavailable"}

    with ThreadPoolExecutor(max_workers=2) as executor:
        bookingsFuture = executor.submit(
            post_json,
            f"{bookingsService}/bookings/create",
            bookingPayload,
        )
        roomsFuture = executor.submit(
            post_json,
            f"{roomsService}/rooms/update",
            roomUpdatePayload,
        )

        bookingsStatus, bookingsData = bookingsFuture.result()
        roomsStatus, roomsData = roomsFuture.result()

    if bookingsStatus >= 400 or roomsStatus >= 400:
        return jsonify({
            "settle": settleData,
            "bookings": {"statusCode": bookingsStatus, "data": bookingsData},
            "rooms": {"statusCode": roomsStatus, "data": roomsData},
            "error": "Finalize step failed after payment settlement",
        }), 502

    loyaltyPublishError = None
    try:
        amountForLoyalty = int(float(bookingPayload["amountSpent"]))
        points = amountForLoyalty // 10
        loyaltyEvent = {
            "event": "booking_paid",
            "email": bookingPayload["customerEmail"],
            "bookingId": str(bookingId),
            "amount": amountForLoyalty,
        }
        getRedisClient().xadd(
            redisStream,
            {"data": json.dumps(loyaltyEvent)},
        )
    except Exception as exc:
        points = None
        loyaltyPublishError = str(exc)

    pendingBookings.pop(intentId, None)

    return jsonify({
        "message": "Booking finalized successfully",
        "settle": settleData,
        "booking": bookingsData,
        "room": roomsData,
        "loyalty": {
            "event": "booking_paid",
            "email": bookingPayload["customerEmail"],
            "points": points,
            "publishError": loyaltyPublishError,
        },
    }), 200



# Cancellation flow
@app.post("/route-bookings/bookings/<int:booking_id>/cancel")
def routeBookingsCancel(booking_id: int):
    unauthorized = checkAuth()
    if unauthorized:
        return unauthorized

    payload = request.get_json(silent=True) or {}
    reason = str(payload.get("reason") or "BOOKING_CANCELLED")
    user = getUser() or {}
    userIdentifier = (user.get("username") or "").strip().lower()
    userRole = str(user.get("role") or "").strip().lower()

    bookingStatus, bookingData = requestJson(
        "GET",
        f"{bookingsService}/bookings/{booking_id}",
    )
    if bookingStatus >= 400:
        return jsonify({
            "error": "Failed to retrieve booking before cancellation",
            "bookings": {"statusCode": bookingStatus, "data": bookingData},
        }), bookingStatus

    if not isinstance(bookingData, dict):
        return jsonify({"error": "Unexpected booking payload from bookings service"}), 502

    holdId = bookingData.get("hold_id") or bookingData.get("holdId")
    legacyHoldId = bookingData.get("legacy_hold_id") or bookingData.get("legacyHoldId")
    amountSpent = bookingData.get("amount_spent") or bookingData.get("amountSpent")
    customerEmail = bookingData.get("customer_email") or bookingData.get("customerEmail")
    roomType = bookingData.get("room_type") or bookingData.get("roomType") or "room"
    checkIn = bookingData.get("check_in") or bookingData.get("checkIn") or "N/A"
    checkOut = bookingData.get("check_out") or bookingData.get("checkOut") or "N/A"

    missingFields = [
        key
        for key, value in {
            "holdId": holdId,
            "amountSpent": amountSpent,
            "customerEmail": customerEmail,
        }.items()
        if value in (None, "")
    ]
    if missingFields:
        return jsonify({
            "error": "Booking record is missing required cancellation fields",
            "missingFields": missingFields,
            "booking": bookingData,
        }), 502

    bookingOwner = str(customerEmail).strip().lower()
    if userRole != "admin" and bookingOwner != userIdentifier:
        return jsonify({
            "error": "Forbidden: booking does not belong to the authenticated user",
            "bookingId": booking_id,
            "bookingOwner": customerEmail,
            "authenticatedUser": user.get("username"),
        }), 403

    try:
        amountSpentInt = int(float(amountSpent))
    except (TypeError, ValueError):
        return jsonify({"error": "amountSpent must be numeric in booking record"}), 502

    refundHoldIds = [str(holdId)]
    if legacyHoldId and str(legacyHoldId) != str(holdId):
        refundHoldIds.append(str(legacyHoldId))

    refundPlan, refundPlanError, planWarnings = computeRefundSplit(refundHoldIds, amountSpentInt)
    if refundPlanError:
        return jsonify({
            "error": "Cancellation failed while planning refund reconciliation",
            "booking": bookingData,
            "details": refundPlanError,
        }), 502

    refundResults = []
    for step in (refundPlan or {}).get("split", []):
        refundPayload = {
            "holdId": step["holdId"],
            "amount": step["amount"],
            "reason": reason,
        }
        refundStatus, refundData = requestJson(
            "POST",
            f"{paymentService}/payments/refund",
            refundPayload,
        )
        refundResults.append({
            "holdId": step["holdId"],
            "requestedAmount": step["amount"],
            "statusCode": refundStatus,
            "data": refundData,
        })

        if refundStatus >= 400:
            return jsonify({
                "error": "Cancellation failed during reconciled refund",
                "booking": bookingData,
                "refundPlan": refundPlan,
                "refundAttempt": refundResults,
            }), 502

    refundSummary = {
        "strategy": "reconciled_by_hold",
        "targetAmount": amountSpentInt,
        "holdIds": refundHoldIds,
        "plan": refundPlan,
        "attempts": refundResults,
    }

    roomUpdatePayload = {
        "holdId": holdId,
        "status": "available",
    }
    roomsStatus, roomsData = requestJson(
        "POST",
        f"{roomsService}/rooms/update",
        roomUpdatePayload,
    )

    roomsErrorText = ""
    if isinstance(roomsData, dict):
        roomsErrorText = str(roomsData.get("error") or roomsData.get("message") or "").strip().lower()

    roomAlreadyMissing = roomsStatus == 404 and "room not found for given holdid" in roomsErrorText
    if roomsStatus >= 400 and not roomAlreadyMissing:
        return jsonify({
            "error": "Cancellation failed during refund or room update",
            "booking": bookingData,
            "refund": refundSummary,
            "rooms": {"statusCode": roomsStatus, "data": roomsData},
        }), 502

    cancellationWarnings = list(planWarnings or [])
    if (refundPlan or {}).get("shortfall", 0) > 0:
        cancellationWarnings.append(
            f"Refund shortfall remains: {(refundPlan or {}).get('shortfall')} after reconciliation"
        )
    if roomAlreadyMissing:
        cancellationWarnings.append("Room holdId not found; treated as already released")

    deleteStatus, deleteData = requestJson(
        "DELETE",
        f"{bookingsService}/bookings/{booking_id}",
    )
    if deleteStatus >= 400:
        return jsonify({
            "error": "Booking cancellation partially completed; booking delete failed",
            "booking": bookingData,
            "refund": refundSummary,
            "rooms": {"statusCode": roomsStatus, "data": roomsData},
            "bookingsDelete": {"statusCode": deleteStatus, "data": deleteData},
        }), 502

    with ThreadPoolExecutor(max_workers=2) as executor:
        notificationFuture = executor.submit(
            enqueueCancellationEmail,
            customerEmail,
            booking_id,
            roomType,
            checkIn,
            checkOut,
        )
        loyaltyFuture = executor.submit(
            publishBookingCancelledEvent,
            customerEmail,
            booking_id,
            amountSpent,
        )

        notificationResult = notificationFuture.result()
        loyaltyResult = loyaltyFuture.result()

    return jsonify({
        "message": "Booking cancelled successfully",
        "booking": bookingData,
        "refund": refundSummary,
        "room": roomsData,
        "warnings": cancellationWarnings,
        "bookingsDelete": deleteData,
        "notification": notificationResult,
        "loyalty": loyaltyResult,
    }), 200


# Modification flow

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
    oldBookingId = payload.get("old_booking_id")
    newRoomType = payload.get("new_room_type")
    newCheckIn = payload.get("new_check_in")
    newCheckOut = payload.get("new_check_out")
    user = getUser() or {}
    userIdentifier = (user.get("username") or "").strip().lower()
    userRole = str(user.get("role") or "").strip().lower()

    if not oldBookingId or not newRoomType or not newCheckIn or not newCheckOut:
        return jsonify({
            "error": "old_booking_id, new_room_type, new_check_in, new_check_out are required"
        }), 400

    bookingStatus, bookingData = requestJson(
        "GET",
        f"{bookingsService}/bookings/{oldBookingId}",
    )
    if bookingStatus >= 400:
        return jsonify({
            "error": "Failed to retrieve old booking",
            "bookings": {"statusCode": bookingStatus, "data": bookingData},
        }), bookingStatus

    if not isinstance(bookingData, dict):
        return jsonify({"error": "Unexpected booking payload"}), 502

    oldHoldId = bookingData.get("hold_id") or bookingData.get("holdId")
    oldAmountSpent = bookingData.get("amount_spent") or bookingData.get("amountSpent")
    customerEmail = bookingData.get("customer_email") or bookingData.get("customerEmail")
    oldRoomType = bookingData.get("room_type") or bookingData.get("roomType")

    if not oldHoldId or oldAmountSpent is None or not customerEmail:
        return jsonify({
            "error": "Old booking missing required fields for modification",
            "missingFields": [k for k in ["holdId", "amountSpent", "customerEmail"] 
                            if bookingData.get(k) is None]
        }), 502

    bookingOwner = str(customerEmail).strip().lower()
    if userRole != "admin" and bookingOwner != userIdentifier:
        return jsonify({
            "error": "Forbidden: booking does not belong to the authenticated user",
            "bookingId": oldBookingId,
            "bookingOwner": customerEmail,
            "authenticatedUser": user.get("username"),
        }), 403

    # Normalize date strings before calling rooms service.
    normalizedCheckIn = normalizeDateString(newCheckIn)
    normalizedCheckOut = normalizeDateString(newCheckOut)
    
    if not normalizedCheckIn or not normalizedCheckOut:
        return jsonify({
            "error": "Invalid date format for check-in or check-out",
            "received": {
                "new_check_in": newCheckIn,
                "new_check_out": newCheckOut,
            },
            "expected_format": "YYYY-MM-DD or RFC format (e.g., 'Wed, 25 Mar 2026 00:00:00 GMT')"
        }), 400

    # Release old hold before creating new hold.
    oldReleasePayload = {
        "holdId": oldHoldId,
        "status": "available",
    }
    oldReleaseStatus, oldReleaseData = requestJson(
        "POST",
        f"{roomsService}/rooms/update",
        oldReleasePayload,
    )
    if oldReleaseStatus >= 400:
        return jsonify({
            "error": "Failed to release old hold for modification",
            "rooms": {"statusCode": oldReleaseStatus, "data": oldReleaseData},
        }), oldReleaseStatus

    newHoldPayload = {
        "roomType": newRoomType,
        "checkIn": normalizedCheckIn,
        "checkOut": normalizedCheckOut,
        "customerEmail": customerEmail,
        "customerMobile": bookingData.get("customer_mobile") or bookingData.get("customerMobile"),
    }

    newHoldStatus, newHoldData = requestJson(
        "POST",
        f"{roomsService}/rooms/holds",
        newHoldPayload,
    )
    if newHoldStatus >= 400:
        # Re-establish old hold if new hold creation fails.
        reHoldPayload = {
            "holdId": oldHoldId,
            "status": "held",
        }
        requestJson(
            "POST",
            f"{roomsService}/rooms/update",
            reHoldPayload,
        )
        return jsonify({
            "error": "Failed to create new hold for proposed modification",
            "rooms": {"statusCode": newHoldStatus, "data": newHoldData},
            "sentPayload": newHoldPayload,
            "recoveryAttempted": "Old hold has been re-established",
        }), newHoldStatus

    newHoldId = newHoldData.get("holdId")
    newAmount = newHoldData.get("amount")

    if not newHoldId or newAmount is None:
        return jsonify({
            "error": "New hold response missing required fields"
        }), 502

    oldAmount = int(float(oldAmountSpent))
    newAmountInt = int(float(newAmount))
    difference = newAmountInt - oldAmount
    loyaltyDelta = difference // 10

    return jsonify({
        "message": "Modification preview generated successfully",
        "old_booking": {
            "bookingId": oldBookingId,
            "roomType": oldRoomType,
            "cost": oldAmount,
            "holdId": oldHoldId,
        },
        "new_booking": {
            "roomType": newRoomType,
            "cost": newAmountInt,
            "holdId": newHoldId,
            "checkIn": newCheckIn,
            "checkOut": newCheckOut,
        },
        "price_adjustment": {
            "old_cost": oldAmount,
            "new_cost": newAmountInt,
            "difference": difference,
            "action": "charge" if difference > 0 else ("refund" if difference < 0 else "none"),
            "loyalty_delta_points": loyaltyDelta,
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
    oldBookingId = payload.get("old_booking_id")
    newBookingId = payload.get("new_booking_id")
    oldHoldIdInput = payload.get("old_hold_id")
    newHoldId = payload.get("new_hold_id")
    cardNumber = payload.get("card_number")
    user = getUser() or {}
    userIdentifier = (user.get("username") or "").strip().lower()
    userRole = str(user.get("role") or "").strip().lower()

    if not oldBookingId or not newHoldId:
        return jsonify({
            "error": "old_booking_id and new_hold_id are required"
        }), 400

    bookingStatus, bookingData = requestJson(
        "GET",
        f"{bookingsService}/bookings/{oldBookingId}",
    )
    if bookingStatus >= 400:
        return jsonify({
            "error": "Failed to retrieve old booking",
            "bookings": {"statusCode": bookingStatus, "data": bookingData},
        }), bookingStatus

    if not isinstance(bookingData, dict):
        return jsonify({"error": "Unexpected booking payload"}), 502

    customerEmail = bookingData.get("customer_email") or bookingData.get("customerEmail")
    oldAmountSpent = bookingData.get("amount_spent") or bookingData.get("amountSpent")
    oldHoldId = bookingData.get("hold_id") or bookingData.get("holdId")

    if not customerEmail or oldAmountSpent is None or not oldHoldId:
        return jsonify({
            "error": "Old booking missing required fields for modification",
            "booking": bookingData,
        }), 502

    if oldHoldIdInput and str(oldHoldIdInput) != str(oldHoldId):
        return jsonify({
            "error": "old_hold_id does not match booking record",
            "providedOldHoldId": oldHoldIdInput,
            "expectedOldHoldId": oldHoldId,
        }), 400

    bookingOwner = str(customerEmail).strip().lower()
    if userRole != "admin" and bookingOwner != userIdentifier:
        return jsonify({
            "error": "Forbidden: booking does not belong to the authenticated user",
            "bookingId": oldBookingId,
        }), 403

    newAmountSpent = payload.get("new_amount_spent")
    if newAmountSpent is None:
        return jsonify({
            "error": "new_amount_spent is required. Use /route-rooms/modify response value."
        }), 400

    try:
        newAmountSpent = int(float(newAmountSpent))
    except (TypeError, ValueError):
        return jsonify({"error": "new_amount_spent must be numeric"}), 400

    if newAmountSpent <= 0:
        return jsonify({"error": "new_amount_spent must be positive"}), 400

    oldAmount = int(float(oldAmountSpent))
    difference = newAmountSpent - oldAmount
    loyaltyDelta = difference // 10

    settlePaymentStatus = None
    settlePaymentData = None
    refundStatus = None
    refundData = None

    if difference > 0:
        if not cardNumber:
            return jsonify({
                "error": "card_number required for additional payment",
                "difference": difference,
            }), 400

        settlePayload = {
            "holdId": newHoldId,
            "amount": difference,
            "card_number": cardNumber,
        }
        settlePaymentStatus, settlePaymentData = requestJson(
            "POST",
            f"{paymentService}/payments/settle-card",
            settlePayload,
        )
        if settlePaymentStatus >= 400:
            return jsonify({
                "error": "Payment settlement failed",
                "payment": {"statusCode": settlePaymentStatus, "data": settlePaymentData},
            }), 502

        if (settlePaymentData.get("status") or "").upper() != "PAID":
            return jsonify({
                "error": "Payment not marked as PAID",
                "payment": settlePaymentData,
            }), 400

    elif difference < 0:
        refundPayload = {
            "holdId": oldHoldId,
            "amount": abs(difference),
            "reason": "BOOKING_MODIFIED",
        }
        refundStatus, refundData = requestJson(
            "POST",
            f"{paymentService}/payments/refund",
            refundPayload,
        )
        if refundStatus >= 400:
            return jsonify({
                "error": "Refund issuance failed",
                "refund": {"statusCode": refundStatus, "data": refundData},
            }), 502

    if not newBookingId:
        newBookingId = int(time.time() * 1000) % 2147483647

    # Normalize date strings for the booking payload.
    normalizedCheckIn = normalizeDateString(payload.get("new_check_in"))
    normalizedCheckOut = normalizeDateString(payload.get("new_check_out"))
    
    if not normalizedCheckIn or not normalizedCheckOut:
        return jsonify({
            "error": "new_check_in and new_check_out are required in confirm step",
            "received": {
                "new_check_in": payload.get("new_check_in"),
                "new_check_out": payload.get("new_check_out"),
            }
        }), 400

    # Resolve room details for the new hold.
    roomContext = getHoldContextFromRooms(newHoldId)
    roomId = roomContext.get("roomID") or payload.get("roomID")
    if not roomId:
        roomId = bookingData.get("room_id") or bookingData.get("roomID")

    newRoomType = payload.get("new_room_type") or bookingData.get("room_type") or bookingData.get("roomType")

    newBookingPayload = {
        "id": newBookingId,
        "roomID": roomId,
        "roomType": newRoomType,
        "customerEmail": customerEmail,
        "customerMobile": bookingData.get("customer_mobile") or bookingData.get("customerMobile"),
        "checkIn": normalizedCheckIn,
        "checkOut": normalizedCheckOut,
        "amountSpent": newAmountSpent,
        "holdId": newHoldId,
        "legacyHoldId": oldHoldId,
    }

    missingBookingFields = [
        key for key in [
            "roomID",
            "roomType",
            "customerEmail",
            "customerMobile",
            "checkIn",
            "checkOut",
            "amountSpent",
            "holdId",
        ] if newBookingPayload.get(key) in (None, "")
    ]
    if missingBookingFields:
        return jsonify({
            "error": "Missing required booking fields for modification confirm",
            "missingFields": missingBookingFields,
        }), 400

    roomConfirmPayload = {
        "holdId": newHoldId,
        "status": "reserved",
        "bookingId": newBookingId,
    }

    # Old hold is released during preview; confirm only the new booking and hold.
    with ThreadPoolExecutor(max_workers=2) as executor:
        newBookingFuture = executor.submit(
            requestJson,
            "POST",
            f"{bookingsService}/bookings/create",
            newBookingPayload,
        )
        roomConfirmFuture = executor.submit(
            requestJson,
            "POST",
            f"{roomsService}/rooms/update",
            roomConfirmPayload,
        )

        newBookingStatus, newBookingData = newBookingFuture.result()
        roomConfirmStatus, roomConfirmData = roomConfirmFuture.result()

    if newBookingStatus >= 400 or roomConfirmStatus >= 400:
        return jsonify({
            "error": "Modification failed during booking/room updates",
            "new_booking": {"statusCode": newBookingStatus, "data": newBookingData},
            "room_confirm": {"statusCode": roomConfirmStatus, "data": roomConfirmData},
        }), 502

    deleteOldBookingStatus, deleteOldBookingData = requestJson(
        "DELETE",
        f"{bookingsService}/bookings/{oldBookingId}",
    )
    if deleteOldBookingStatus >= 400:
        return jsonify({
            "error": "Old booking deletion failed after modification",
            "new_booking": {"statusCode": newBookingStatus, "data": newBookingData},
            "delete_old": {"statusCode": deleteOldBookingStatus, "data": deleteOldBookingData},
        }), 502

    loyalty_event = {
        "event": "booking_modified",
        "email": customerEmail,
        "bookingId": str(newBookingId),
        "old_amount": oldAmount,
        "new_amount": newAmountSpent,
    }
    
    loyalty_publish_error = None
    try:
        getRedisClient().xadd(
            redisStream,
            {"data": json.dumps(loyalty_event)},
        )
    except Exception as exc:
        loyalty_publish_error = str(exc)

    return jsonify({
        "message": "Booking modified successfully",
        "old_booking": {
            "bookingId": oldBookingId,
            "cost": oldAmount,
        },
        "new_booking": newBookingData,
        "payment_settlement": {
            "difference": difference,
            "action": "charge" if difference > 0 else ("refund" if difference < 0 else "none"),
            "payment": settlePaymentData if difference > 0 else None,
            "refund": refundData if difference < 0 else None,
        },
        "rooms": {
            "old_released": None,
            "new_confirmed": roomConfirmData,
        },
        "loyalty": {
            "event": "booking_modified",
            "email": customerEmail,
            "old_amount": oldAmount,
            "new_amount": newAmountSpent,
            "points_delta": loyaltyDelta,
            "publishError": loyalty_publish_error,
        },
    }), 200


# Support and utility endpoints
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

    status_code, data = requestJson(
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
    holdId = payload.get("holdId")
    amount = payload.get("amount")
    cardNumber = payload.get("card_number")
    customerEmail = str(payload.get("customerEmail") or "").strip()
    customerMobile = str(payload.get("customerMobile") or "").strip()

    if not holdId:
        return jsonify({"error": "holdId is required"}), 400
    if amount is None:
        return jsonify({"error": "amount is required"}), 400
    if not cardNumber:
        return jsonify({"error": "card_number is required"}), 400
    if not customerEmail:
        return jsonify({"error": "customerEmail is required"}), 400
    if not customerMobile:
        return jsonify({"error": "customerMobile is required"}), 400

    context = pendingCardBookings.get(str(holdId), {}).copy()
    if not context:
        context = getHoldContextFromRooms(holdId)

    settleAmount = amount
    contextAmount = context.get("amount")
    if contextAmount is not None:
        try:
            requestedAmount = int(float(amount))
            expectedAmount = int(float(contextAmount))
        except (TypeError, ValueError):
            return jsonify({"error": "amount must be numeric"}), 400

        if requestedAmount != expectedAmount:
            return jsonify({
                "error": "amount does not match hold amount",
                "requestedAmount": requestedAmount,
                "expectedAmount": expectedAmount,
                "holdId": holdId,
            }), 400

        settleAmount = expectedAmount

    settleStatus, settleData = requestJson(
        "POST",
        f"{paymentService}/payments/settle-card",
        {
            "holdId": holdId,
            "amount": settleAmount,
            "card_number": cardNumber,
        },
    )
    if settleStatus >= 400:
        return jsonify(settleData), settleStatus

    if (settleData.get("status") or "").upper() != "PAID":
        return jsonify({
            "error": "Card payment not marked as PAID",
            "payment": settleData,
        }), 400

    providedBookingId = payload.get("bookingId")
    if providedBookingId is not None:
        try:
            bookingId = int(providedBookingId)
        except (TypeError, ValueError):
            return jsonify({"error": "bookingId must be an integer"}), 400
    else:
        bookingId = int(time.time() * 1000) % 2147483647

    bookingPayload = {
        "id": bookingId,
        "roomID": payload.get("roomID") or context.get("roomID"),
        "roomType": payload.get("roomType") or context.get("roomType"),
        "customerEmail": customerEmail,
        "customerMobile": customerMobile,
        "checkIn": payload.get("checkIn") or context.get("checkIn"),
        "checkOut": payload.get("checkOut") or context.get("checkOut"),
        "amountSpent": payload.get("amountSpent") or context.get("amount") or amount,
        "holdId": holdId,
    }

    missingBookingFields = [
        key for key in [
            "roomID",
            "roomType",
            "customerEmail",
            "customerMobile",
            "checkIn",
            "checkOut",
            "amountSpent",
            "holdId",
        ] if bookingPayload.get(key) in (None, "")
    ]
    if missingBookingFields:
        return jsonify({
            "message": "Payment settled, but booking was not auto-created due to missing fields",
            "payment": settleData,
            "autoBookingCreated": False,
            "missingFields": missingBookingFields,
            "hint": "Pass complete booking fields in settle-card request (customerEmail/customerMobile are now mandatory)",
        }), 202

    roomUpdatePayload = {
        "holdId": holdId,
        "status": payload.get("roomStatus", "reserved"),
        "bookingId": bookingId,
        "reservationDate": payload.get("reservationDate"),
    }

    with ThreadPoolExecutor(max_workers=2) as executor:
        bookingsFuture = executor.submit(
            requestJson,
            "POST",
            f"{bookingsService}/bookings/create",
            bookingPayload,
        )
        roomsFuture = executor.submit(
            requestJson,
            "POST",
            f"{roomsService}/rooms/update",
            roomUpdatePayload,
        )

        bookingsStatus, bookingsData = bookingsFuture.result()
        roomsStatus, roomsData = roomsFuture.result()

    if bookingsStatus >= 400 or roomsStatus >= 400:
        return jsonify({
            "error": "Card settlement succeeded but booking finalization failed",
            "payment": settleData,
            "bookings": {"statusCode": bookingsStatus, "data": bookingsData},
            "rooms": {"statusCode": roomsStatus, "data": roomsData},
        }), 502

    loyaltyPublishError = None
    try:
        amountForLoyalty = int(float(bookingPayload["amountSpent"]))
        points = amountForLoyalty // 10
        loyaltyEvent = {
            "event": "booking_paid",
            "email": bookingPayload["customerEmail"],
            "bookingId": str(bookingId),
            "amount": amountForLoyalty,
        }
        getRedisClient().xadd(redisStream, {"data": json.dumps(loyaltyEvent)})
    except Exception as exc:
        points = None
        loyaltyPublishError = str(exc)

    pendingCardBookings.pop(str(holdId), None)

    return jsonify({
        "message": "Card settled and booking finalized successfully",
        "payment": settleData,
        "booking": bookingsData,
        "room": roomsData,
        "loyalty": {
            "event": "booking_paid",
            "email": bookingPayload["customerEmail"],
            "points": points,
            "publishError": loyaltyPublishError,
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

    hold_status, hold_data = requestJson("POST", f"{roomsService}/rooms/holds", payload)

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


@app.get("/route-loyalty/my-points")
def routeLoyaltyMyPoints():
    """
    Return a simple loyalty points summary for the authenticated user.
    """
    unauthorized = checkAuth()
    if unauthorized:
        return unauthorized

    user = getUser() or {}
    user_identifier = (user.get("email") or user.get("username") or "").strip()
    if not user_identifier:
        return jsonify({"error": "Authenticated user missing email/username"}), 400

    status_code, bookings_data = requestJson(
        "GET",
        f"{bookingsService}/bookings",
        params={"customerEmail": user_identifier},
    )
    if status_code >= 400:
        return jsonify({
            "error": "Failed to fetch bookings for loyalty summary",
            "bookings": {"statusCode": status_code, "data": bookings_data},
        }), status_code

    if not isinstance(bookings_data, list):
        return jsonify({"error": "Unexpected bookings payload for loyalty summary"}), 502

    total_spent = 0
    for booking in bookings_data:
        if not isinstance(booking, dict):
            continue
        amount_raw = booking.get("amount_spent") or booking.get("amountSpent") or 0
        try:
            total_spent += int(float(amount_raw))
        except (TypeError, ValueError):
            continue

    return jsonify({
        "email": user_identifier,
        "bookingCount": len(bookings_data),
        "totalSpent": total_spent,
        "loyaltyPoints": total_spent // 10,
    }), 200


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

    getRedisClient().xadd(
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

    getRedisClient().xadd(
        redisStream,
        {
            "event": "deduct_points",
            "email": email,
            "points": str(points),
        },
    )

    return jsonify({"status": "queued", "event": "deduct_points"}), 202


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, threaded=True)
