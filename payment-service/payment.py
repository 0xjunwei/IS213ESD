import os
import time
import secrets
from decimal import Decimal, ROUND_DOWN
import requests
import psycopg2
import psycopg2.extras
from flask import Flask, request, jsonify

app = Flask(__name__)

# Config
ETHERSCAN_V2_HOST = os.getenv("ETHERSCAN_V2_HOST", "https://api.etherscan.io/v2/api")
CHAIN_ID = os.getenv("CHAIN_ID", "84532")  # Base Sepolia
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY", "")

PAY_TO_ADDRESS = os.getenv("PAY_TO_ADDRESS", "0x56BC54Ad0A2470336220E4204f14B512766b5410")
TOKEN_CONTRACT = os.getenv("TOKEN_CONTRACT", "0xbbD47B1eAdb7513a08c26C68E2669f4FE3B7Eae7")
TOKEN_DECIMALS = int(os.getenv("TOKEN_DECIMALS", "6"))

TIME_WINDOW_SECONDS = int(os.getenv("TIME_WINDOW_SECONDS", "3600"))

DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "payments_db")
DB_USER = os.getenv("DB_USER", "payments_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "payments_pass")

# Keccak256 hash of event log "Transfer(address,address,uint256)" from ERC20 transfers
TRANSFER_TOPIC0 = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


def get_db_connection():
    """
    To connect to the db
    """
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )


def init_db():
    """
    On docker compose, we will set up the tables within the db, payments + refund table so that its clean, filter for payments
    Searching for payments WHERE hold_id = 'xxx'
    """
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS payments (
            payment_id SERIAL PRIMARY KEY,
            intent_id VARCHAR(120) UNIQUE NOT NULL,
            hold_id VARCHAR(120),
            booking_id VARCHAR(120),
            payment_method VARCHAR(20) NOT NULL,
            payment_type VARCHAR(20) NOT NULL DEFAULT 'CHARGE',
            amount NUMERIC(18,6) NOT NULL,
            currency VARCHAR(10) NOT NULL DEFAULT 'SGD',
            status VARCHAR(20) NOT NULL,
            txn_id VARCHAR(120),
            payer_address VARCHAR(120),
            time_created TIMESTAMP NOT NULL DEFAULT NOW(),
            paid_at TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT NOW()
        );
        """
    )
    conn.commit()
    cur.close()
    conn.close()


@app.get("/health")
def health():
    """
    Basic health check similar to the lab
    """
    return jsonify(status="ok"), 200


def normalize_card_number(value):
    """
    checks the card number if all of them are digits, for sanitization sake
    """
    if value is None:
        return ""
    return "".join(ch for ch in str(value) if ch.isdigit())


def luhn_check(number):
    """
    Following: https://www.geeksforgeeks.org/dsa/luhn-algorithm/
    Basic Luhn check to return 200/400 for card payments
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


def parse_amount(value):
    """
    Parse the amount to a decimal with 2 decimal places, rounding down. Return None if invalid.
    """
    try:
        return Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
    except Exception:
        return None


def make_unique_amount(base_amount):
    """
    Thought about replay attacks by malicious actors, i cant use the calldata to pass intentID within the transaction
    Due to ERC20 Standards, have to utilize the transfer function, thus parameters are set in place.
    Will utilize Stablecoins having 6 decimal places as a unique identifier whereby the users amount e.g. 200.00 will be placed with a unique
    4 digits suffix, which be checked against and payment must be made within 5 minutes to be valid.
    Not perfect but PoC, alternative is to utilize an contract factory CREATE2 for unique addresses per payment intent
    """
    suffix = secrets.randbelow(10000)
    suffix_decimal = Decimal(suffix) / Decimal("10000")
    unique_amount = base_amount + (suffix_decimal / Decimal("100"))
    return unique_amount.quantize(Decimal("0.000001"))


def amount_to_base_units(amount_value):
    """
    Convert a decimal amount to base units (e.g., for a token with 6 decimals, 1.234567 becomes 1234567).
    There is no float in Solidity, so we convert the amount to a full integer to pad
    """
    return int(Decimal(str(amount_value)) * (Decimal(10) ** TOKEN_DECIMALS))


def topic_to_address(topic_hex):
    """
    Convert a 32-byte topic hex string to an Ethereum address (last 20 bytes).
    """
    if not topic_hex or not topic_hex.startswith("0x") or len(topic_hex) != 66:
        return ""
    return ("0x" + topic_hex[-40:]).lower()


def basescan_proxy(action, extra_params):
    """
    Helper function to call BaseScan API with common parameters, 
    based on their docs the action would be the api function i need to call
    """
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
    """
    Get transaction receipt from BaseScan API for a given transaction hash.
    """
    return basescan_proxy("eth_getTransactionReceipt", {"txhash": tx_hash})


def create_payment_record(
    intent_id,
    hold_id,
    booking_id,
    payment_method,
    amount,
    status,
    currency="SGD",
    payment_type="CHARGE",
    txn_id=None,
    payer_address=None
):
    """
    Helper function to create a payment record in the database with all relevant details.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO payments (
            intent_id, hold_id, booking_id, payment_method, payment_type,
            amount, currency, status, txn_id, payer_address
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            intent_id,
            hold_id,
            booking_id,
            payment_method,
            payment_type,
            amount,
            currency,
            status,
            txn_id,
            payer_address,
        ),
    )
    conn.commit()
    cur.close()
    conn.close()


def get_payment_by_intent(intent_id):
    """
    Helper Function to retrieve the Intent ID for settlement
    """
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT * FROM payments WHERE intent_id = %s", (intent_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row


def mark_payment_paid(intent_id, txn_id=None, payer_address=None):
    """
    Helper function as name implies to mark payment as paid, txn_id can be none as payment could be a card payment
    Thus simulated
    """
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE payments
        SET status = 'PAID',
            txn_id = %s,
            payer_address = %s,
            paid_at = NOW(),
            updated_at = NOW()
        WHERE intent_id = %s
        """,
        (txn_id, payer_address, intent_id),
    )
    conn.commit()
    cur.close()
    conn.close()


def mark_payment_failed(intent_id):
    """
    Helper function as name implies, if payment fails
    """
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE payments
        SET status = 'FAILED',
            updated_at = NOW()
        WHERE intent_id = %s
        """,
        (intent_id,),
    )
    conn.commit()
    cur.close()
    conn.close()


@app.post("/payments/settle-card")
def settle_card():
    """
    Card settlement function, simulated using luhn algorithm to check validity
    """
    if request.content_type != "application/json":
        return jsonify(error="Content-Type must be application/json"), 415

    data = request.get_json(silent=True) or {}
    hold_id = data.get("holdId")
    amount = data.get("amount")
    card_number_raw = data.get("card_number")

    if not hold_id:
        return jsonify(status="FAILED", error="holdId is required"), 422
    if amount is None:
        return jsonify(status="FAILED", error="amount is required"), 422

    card_number = normalize_card_number(card_number_raw)
    intent_id = "card_" + str(hold_id) + "_" + str(int(time.time() * 1000))

    if (
        not card_number
        or len(card_number) < 13
        or len(card_number) > 19
        or not luhn_check(card_number)
    ):
        create_payment_record(
            intent_id=intent_id,
            hold_id=hold_id,
            booking_id=None,
            payment_method="CARD_LUHN",
            amount=amount,
            status="FAILED",
        )
        return jsonify(status="FAILED", error="credit card is invalid"), 402

    create_payment_record(
        intent_id=intent_id,
        hold_id=hold_id,
        booking_id=None,
        payment_method="CARD_LUHN",
        amount=amount,
        status="PAID",
    )

    mark_payment_paid(intent_id)

    return jsonify(
        status="PAID",
        method="CARD_LUHN",
        paymentIntentId=intent_id,
        holdId=hold_id
    ), 200


@app.post("/payments/create-intents")
def initiate_agent_payment():
    """
    For stablecoin payment, in our scenario it be for agentic AI, we will first create the intent for payment
    Generating an unique amount for user to pay, along with passing the user/agent the address to pay to.
    """
    if request.content_type != "application/json":
        return jsonify(error="Content-Type must be application/json"), 415

    if not PAY_TO_ADDRESS:
        return jsonify(error="PAY_TO_ADDRESS not configured"), 500

    data = request.get_json(silent=True) or {}
    hold_id = data.get("holdId")
    amount = parse_amount(data.get("amount"))

    if not hold_id:
        return jsonify(error="holdId is required"), 422
    if amount is None or amount <= 0:
        return jsonify(error="amount must be positive"), 422

    unique_amount = make_unique_amount(amount)
    intent_id = "agent_" + str(hold_id) + "_" + str(int(time.time() * 1000))

    create_payment_record(
        intent_id=intent_id,
        hold_id=hold_id,
        booking_id=None,
        payment_method="STABLECOIN",
        amount=str(unique_amount),
        status="PENDING",
    )

    return (
        jsonify(
            paymentIntentId=intent_id,
            holdId=hold_id,
            payTo=PAY_TO_ADDRESS,
            amount=str(unique_amount),
            network="base-sepolia",
        ),
        201,
    )


@app.post("/payments/settle")
def settle_agent_payment():
    """
    Agent/User will pass the transaction hash of the payment made, along with the original Intent ID
    We will validate through RPC call to etherscan for the transaction block, and search if transaction is valid
    Once validated we would mark payment as paid.
    """
    if request.content_type != "application/json":
        return jsonify(error="Content-Type must be application/json"), 415

    data = request.get_json(silent=True) or {}
    intent_id = (data.get("paymentIntentId") or "").strip()
    tx_hash = (data.get("tx_hash") or "").strip()

    if not intent_id or not tx_hash:
        return jsonify(status="FAILED"), 400

    payment = get_payment_by_intent(intent_id)
    if not payment:
        return jsonify(status="FAILED"), 400

    if payment["status"] == "PAID":
        stored_tx = payment.get("txn_id")
        if stored_tx and stored_tx != tx_hash:
            return jsonify(status="FAILED"), 400
        return jsonify(status="PAID"), 200

    expected_amount = payment.get("amount")
    if not expected_amount:
        return jsonify(status="FAILED"), 400

    expected_units = amount_to_base_units(expected_amount)
    pay_to = PAY_TO_ADDRESS.lower()
    token_addr = TOKEN_CONTRACT.lower()

    try:
        receipt = (get_tx_receipt(tx_hash) or {}).get("result")
        if not receipt or receipt.get("status") != "0x1":
            mark_payment_failed(intent_id)
            return jsonify(status="FAILED"), 400

        logs = receipt.get("logs") or []
        matched = False
        block_ts = None
        payer_address = None

        for log in logs:
            if (log.get("address") or "").lower() != token_addr:
                continue

            topics = log.get("topics") or []
            if len(topics) < 3:
                continue
            
            if (topics[0] or "").lower() != TRANSFER_TOPIC0:
                continue

            from_addr = topic_to_address(topics[1])
            to_addr = topic_to_address(topics[2])

            if to_addr != pay_to:
                continue

            value_units = int((log.get("data") or "0x0"), 16)
            if value_units != expected_units:
                continue

            ts_hex = log.get("blockTimestamp")
            if ts_hex:
                block_ts = int(ts_hex, 16)

            payer_address = from_addr
            matched = True
            break

        if not matched or block_ts is None:
            mark_payment_failed(intent_id)
            return jsonify(status="FAILED"), 400

        created_ts = int(payment["time_created"].timestamp())
        if block_ts < created_ts or block_ts - created_ts > TIME_WINDOW_SECONDS:
            mark_payment_failed(intent_id)
            return jsonify(status="FAILED"), 400

    except Exception:
        mark_payment_failed(intent_id)
        return jsonify(status="FAILED"), 400

    mark_payment_paid(intent_id, txn_id=tx_hash, payer_address=payer_address)
    return jsonify(status="PAID", method="TX_HASH_PROOF"), 200


@app.get("/payments/<intent_id>")
def get_payment(intent_id):
    """
    For Debugging purpose, will remove once everything is stable, 
    to check the payment record by intent ID, to see the status and details of the payment
    """
    payment = get_payment_by_intent(intent_id)
    if not payment:
        return jsonify(error="payment not found"), 404
    return jsonify(payment), 200


if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=8080, debug=True)
