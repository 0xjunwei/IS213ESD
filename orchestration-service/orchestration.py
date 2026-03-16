import os

import requests
from flask import Flask, jsonify, request

app = Flask(__name__)


# Config (override via env vars as needed)
authService = os.getenv("AUTH_SERVICE", "http://auth-service:5000")
paymentService = os.getenv("PAYMENT_SERVICE", "http://payment-service:5002")
bookingsService = os.getenv("BOOKINGS_SERVICE", "http://bookings-service:5000")
forwardTimeout = float(os.getenv("FORWARD_TIMEOUT", "8"))

# In-memory sessions: token -> user
sessions = {}


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


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
