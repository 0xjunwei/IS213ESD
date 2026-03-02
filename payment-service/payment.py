import os
import json
import time
from decimal import Decimal, ROUND_DOWN
import secrets
import threading
import requests
from flask import Flask, request, jsonify

app = Flask(__name__)


# My code will follow python pep8 standard, for documentation it is not AI Generated
# Format to explain my thought process

# Config
ETHERSCAN_V2_HOST = os.getenv("ETHERSCAN_V2_HOST", "https://api.etherscan.io/v2/api")
CHAIN_ID = os.getenv("CHAIN_ID", "84532")  # Base Sepolia
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY", "")  # same key across chains in V2
PAY_TO_ADDRESS = "0x56BC54Ad0A2470336220E4204f14B512766b5410"
STATE_FILE = "intents_state.json"
# XSGD/USDC contract on Base Sepolia, will create a custom ERC20 contract for this and peg it to 6 decimal simulating USDC
TOKEN_CONTRACT = "0xbbD47B1eAdb7513a08c26C68E2669f4FE3B7Eae7"
TOKEN_DECIMALS = int(os.getenv("TOKEN_DECIMALS", "6"))

# Mutex to protect file read-modify-write
STATE_LOCK = threading.Lock()


@app.get("/health")
def health():
    """
    To get the uptime of the payment processor, incase it is down and we dk
    """
    return jsonify(status="ok"), 200


# Local JSON state storage
def load_state_unlocked():
    if not os.path.exists(STATE_FILE):
        return {}
    with open(STATE_FILE, "r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            return {}


def save_state_unlocked(state):
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, STATE_FILE)


def persist_payment(intent_id, record):
    with STATE_LOCK:
        state = load_state_unlocked()
        state[intent_id] = record
        save_state_unlocked(state)


# LUHNS check for credit card if valid
def normalize_card_number(value):
    if value is None:
        return ""
    return "".join(ch for ch in str(value) if ch.isdigit())


# Referencing to: https://www.geeksforgeeks.org/dsa/luhn-algorithm/
def luhn_check(number):
    """
    Reverse the digits, then iterates through all the digits
    double the value of every second digit
    If results is 2 digits, we sum it tgt
    If the total modulo 10 is equal to 0 (if the total ends in zero)
    then the number is valid according to the Luhn formula; else it is not valid.
    """
    if not number.isdigit():
        return False

    total = 0
    reverse_digits = number[::-1]

    for i, ch in enumerate(reverse_digits):
        digit = int(ch)
        if i % 2 == 1:
            digit *= 2
            if digit > 9:
                digit -= 9
        total += digit
    return (total % 10) == 0


@app.post("/payments/settle-card")
def settle_card():
    if request.content_type != "application/json":
        return jsonify(error="Content-Type must be application/json"), 415

    data = request.get_json(silent=True) or {}
    booking_id = data.get("bookingId")
    amount = data.get("amount")
    card_number_raw = data.get("card_number")

    if not booking_id:
        return jsonify(status="FAILED", error="bookingId is required"), 422
    if amount is None:
        return jsonify(status="FAILED", error="amount is required"), 422

    card_number = normalize_card_number(card_number_raw)

    intent_id = "card_" + str(booking_id) + "_" + str(int(time.time() * 1000))

    if (
        not card_number
        or len(card_number) < 13
        or len(card_number) > 19
        or not luhn_check(card_number)
    ):
        record = {
            "bookingId": str(booking_id),
            "method": "CARD_LUHN",
            "amount": amount,
            "status": "FAILED",
            "createdAt": int(time.time()),
        }
        persist_payment(intent_id, record)

        return jsonify(status="FAILED", error="credit card is invalid"), 402

    record = {
        "bookingId": str(booking_id),
        "method": "CARD_LUHN",
        "amount": amount,
        "status": "PAID",
        "createdAt": int(time.time()),
    }
    persist_payment(intent_id, record)

    return jsonify(status="PAID", method="CARD_LUHN", paymentIntentId=intent_id), 200


TRANSFER_TOPIC0 = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
TIME_WINDOW_SECONDS = 3600  # 1 hour for demo


def topic_to_address(topic_hex):
    if not topic_hex or not topic_hex.startswith("0x") or len(topic_hex) != 66:
        return ""
    return ("0x" + topic_hex[-40:]).lower()


def amount_to_base_units(amount_str):
    return int(Decimal(str(amount_str)) * (Decimal(10) ** TOKEN_DECIMALS))


# Scenario 1: Agentic AI stablecoin PoC (tx hash proof)
# To account for edge case where malicious actor might steal another txnID and use as payment, we would prepend a unique digit behind the amount
# Stablecoins are 6 decimals long, so etc the payment is for 200.00 SGD, we would prepend 200.001234 as the unique identifier
# We could factor that as additional cost of operations < 1 cent.
def parse_amount(value):
    try:
        # Force 2 decimal places for cents
        amount = Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
        return amount
    except Exception:
        return None


def make_unique_amount(base_amount):
    """
    XSGD uses 6 decimals.
    Base amount assumed to already have 2 decimals (e.g. 200.00).
    We append 4 random digits to fill remaining precision.
    Example:
        200.00 -> 200.001234
    """
    suffix = secrets.randbelow(10000)  # 0000 to 9999
    suffix_decimal = Decimal(suffix) / Decimal("10000")  # 0.0000 to 0.9999

    # Shift suffix into 4 decimal positions (after cents)
    unique_amount = base_amount + (suffix_decimal / Decimal("100"))

    # Ensure exactly 6 decimals
    return str(unique_amount.quantize(Decimal("0.000001")))


def basescan_proxy(action, extra_params):
    params = {
        "chainid": CHAIN_ID,
        "module": "proxy",
        "action": action,
        "apikey": ETHERSCAN_API_KEY,
    }
    params.update(extra_params)

    resp = requests.get(ETHERSCAN_V2_HOST, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json()


def get_tx_receipt(tx_hash):
    return basescan_proxy("eth_getTransactionReceipt", {"txhash": tx_hash})


def get_block_by_number(block_number_hex):
    return basescan_proxy(
        "eth_getBlockByNumber", {"tag": block_number_hex, "boolean": "false"}
    )


@app.post("/payments/agent/initiate")
def initiate_agent_payment():
    if request.content_type != "application/json":
        return jsonify(error="Content-Type must be application/json"), 415

    if not PAY_TO_ADDRESS:
        return jsonify(error="PAY_TO_ADDRESS not configured"), 500

    data = request.get_json(silent=True) or {}
    booking_id = data.get("bookingId")
    amount = parse_amount(data.get("amount"))

    if not booking_id:
        return jsonify(error="bookingId is required"), 422
    if amount is None or amount <= 0:
        return jsonify(error="amount must be positive"), 422

    unique_amount = make_unique_amount(amount)
    intent_id = "agent_" + str(booking_id) + "_" + str(int(time.time() * 1000))

    record = {
        "bookingId": str(booking_id),
        "method": "STABLECOIN",
        "amount": unique_amount,
        "status": "PENDING",
        "createdAt": int(time.time()),
        "tx_hash": None,
    }
    persist_payment(intent_id, record)

    return (
        jsonify(
            paymentIntentId=intent_id,
            payTo=PAY_TO_ADDRESS,
            amount=unique_amount,
            network="base-sepolia",
        ),
        201,
    )


@app.post("/payments/agent/settle")
def settle_agent_payment():
    if request.content_type != "application/json":
        return jsonify(error="Content-Type must be application/json"), 415

    data = request.get_json(silent=True) or {}
    intent_id = (data.get("paymentIntentId") or "").strip()
    tx_hash = (data.get("tx_hash") or "").strip()

    if not intent_id or not tx_hash:
        return jsonify(status="FAILED"), 400

    with STATE_LOCK:
        state = load_state_unlocked()
        intent = state.get(intent_id)

    if not intent:
        return jsonify(status="FAILED"), 400

    if intent.get("status") == "PAID":
        return jsonify(status="PAID"), 200

    expected_amount = intent.get("amount")
    created_at = int(intent.get("createdAt") or 0)
    if not expected_amount or not created_at:
        return jsonify(status="FAILED"), 400

    expected_units = amount_to_base_units(expected_amount)
    pay_to = PAY_TO_ADDRESS.lower()
    token_addr = TOKEN_CONTRACT.lower()

    try:
        receipt = (get_tx_receipt(tx_hash) or {}).get("result")
        if not receipt or receipt.get("status") != "0x1":
            return jsonify(status="FAILED"), 400

        logs = receipt.get("logs") or []
        matched = False
        block_ts = None

        for log in logs:
            if (log.get("address") or "").lower() != token_addr:
                continue

            topics = log.get("topics") or []
            if len(topics) < 3:
                continue

            if (topics[0] or "").lower() != TRANSFER_TOPIC0:
                continue

            to_addr = topic_to_address(topics[2])
            if to_addr != pay_to:
                continue

            value_units = int((log.get("data") or "0x0"), 16)
            if value_units != expected_units:
                continue

            # If provided by the API, use this directly
            ts_hex = log.get("blockTimestamp")
            if ts_hex:
                block_ts = int(ts_hex, 16)

            matched = True
            break

        if not matched:
            return jsonify(status="FAILED"), 400

        # time check (relative to intent creation)
        if block_ts is None:
            return jsonify(status="FAILED"), 400

        if block_ts < created_at:
            return jsonify(status="FAILED"), 400

        if block_ts - created_at > TIME_WINDOW_SECONDS:
            return jsonify(status="FAILED"), 400

    except Exception:
        return jsonify(status="FAILED"), 400

    with STATE_LOCK:
        state = load_state_unlocked()
        state[intent_id]["status"] = "PAID"
        state[intent_id]["tx_hash"] = tx_hash
        state[intent_id]["paidAt"] = int(time.time())
        save_state_unlocked(state)

    return jsonify(status="PAID", method="TX_HASH_PROOF"), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True, threaded=True)
