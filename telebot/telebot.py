import json
import os
import re
import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, Optional

import requests
from openai import OpenAI
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters
from web3 import Web3


TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

ORCHESTRATOR_BASE = os.getenv("ORCHESTRATOR_BASE", "http://localhost:8000")
ROOM_BOOK_ROUTE = f"{ORCHESTRATOR_BASE}/route-rooms/book"
ROOM_CONFIRM_ROUTE = f"{ORCHESTRATOR_BASE}/route-rooms/book/confirm"
ROOM_OPTIONS_ROUTE = f"{ORCHESTRATOR_BASE}/route-rooms/options"

RPC_URL = os.getenv("RPC_URL", "https://sepolia.base.org")
TOKEN_CONTRACT = os.getenv("TOKEN_CONTRACT", "0xbbD47B1eAdb7513a08c26C68E2669f4FE3B7Eae7")
TOKEN_DECIMALS = int(os.getenv("TOKEN_DECIMALS", "6"))
CHAIN_ID = int(os.getenv("CHAIN_ID", "84532"))
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "20"))
CONFIRM_MAX_RETRIES = int(os.getenv("CONFIRM_MAX_RETRIES", "6"))
CONFIRM_RETRY_DELAY_SECONDS = float(os.getenv("CONFIRM_RETRY_DELAY_SECONDS", "1.5"))

openai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None


@dataclass
class SessionState:
	slots: Dict[str, Optional[str]] = field(
		default_factory=lambda: {
			"roomType": None,
			"checkIn": None,
			"checkOut": None,
			"customerEmail": None,
			"customerMobile": None,
		}
	)
	collecting: bool = False
	awaiting_payment_confirm: bool = False
	payment_info: Dict[str, Any] = field(default_factory=dict)
	private_key: Optional[str] = None
	party_size: Optional[int] = None


sessions: Dict[int, SessionState] = {}


def get_session(user_id: int) -> SessionState:
	if user_id not in sessions:
		sessions[user_id] = SessionState()
	return sessions[user_id]


def clear_booking_flow(state: SessionState) -> None:
	state.slots = {
		"roomType": None,
		"checkIn": None,
		"checkOut": None,
		"customerEmail": None,
		"customerMobile": None,
	}
	state.collecting = False
	state.awaiting_payment_confirm = False
	state.payment_info = {}
	state.party_size = None


def parse_json_text(raw: str) -> Dict[str, Any]:
	try:
		return json.loads(raw)
	except Exception:
		return {}


def fallback_extract_slots(text: str) -> Dict[str, str]:
	extracted: Dict[str, str] = {}
	date_matches = re.findall(r"\b\d{4}-\d{2}-\d{2}\b", text)
	if len(date_matches) >= 1:
		extracted["checkIn"] = date_matches[0]
	if len(date_matches) >= 2:
		extracted["checkOut"] = date_matches[1]

	if "checkIn" not in extracted or "checkOut" not in extracted:
		natural_date_pattern = (
			r"(\d{1,2}(?:st|nd|rd|th)?\s+[A-Za-z]+\s+\d{4}|"
			r"[A-Za-z]+\s+\d{1,2}(?:st|nd|rd|th)?(?:,)?\s*\d{4})"
		)
		natural_dates = re.findall(natural_date_pattern, text, flags=re.IGNORECASE)
		parsed_dates = [normalize_date_string(item) for item in natural_dates]
		parsed_dates = [item for item in parsed_dates if item]
		if len(parsed_dates) >= 1 and "checkIn" not in extracted:
			extracted["checkIn"] = parsed_dates[0]
		if len(parsed_dates) >= 2 and "checkOut" not in extracted:
			extracted["checkOut"] = parsed_dates[1]

	email_match = re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text)
	if email_match:
		extracted["customerEmail"] = email_match.group(0)

	mobile_match = re.search(r"\b\d{8,15}\b", text)
	if mobile_match:
		extracted["customerMobile"] = mobile_match.group(0)

	room_match = re.search(
		r"\b(Standard|Deluxe|Suite|Single|Double|single room|double room)\b",
		text,
		re.IGNORECASE,
	)
	if room_match:
		extracted["roomType"] = normalize_room_type(room_match.group(1))

	return extracted


def normalize_date_string(raw: str) -> Optional[str]:
	cleaned = raw.strip().lower()
	cleaned = re.sub(r"(\d)(st|nd|rd|th)", r"\1", cleaned)
	cleaned = cleaned.replace(",", " ")
	cleaned = re.sub(r"\s+", " ", cleaned)

	for fmt in ("%d %B %Y", "%d %b %Y", "%B %d %Y", "%b %d %Y"):
		try:
			parsed = datetime.strptime(cleaned, fmt)
			return parsed.strftime("%Y-%m-%d")
		except ValueError:
			continue
	return None


def normalize_room_type(raw: str) -> str:
	mapped = {
		"single": "Standard",
		"single room": "Standard",
		"double": "Deluxe",
		"double room": "Deluxe",
		"standard": "Standard",
		"deluxe": "Deluxe",
		"suite": "Suite",
	}
	return mapped.get(raw.strip().lower(), raw.strip().title())


def detect_booking_intent(text: str) -> bool:
	return bool(
		re.search(
			r"\b(book|booking|reserve|reservation|room)\b",
			text,
			re.IGNORECASE,
		)
	)


def extract_party_size(text: str) -> Optional[int]:
	match = re.search(r"\b(\d{1,2})\s*(person|persons|people|guest|guests|pax)\b", text, re.IGNORECASE)
	if not match:
		return None
	try:
		return int(match.group(1))
	except ValueError:
		return None


def format_date_human(raw_date: Any) -> str:
	if raw_date is None:
		return "N/A"
	text = str(raw_date)
	for fmt in ("%Y-%m-%d", "%a, %d %b %Y %H:%M:%S GMT"):
		try:
			parsed = datetime.strptime(text, fmt)
			return parsed.strftime("%d %b %Y")
		except ValueError:
			continue
	return text


def format_book_intent_success_message(book_payload: Dict[str, Any], book_data: Dict[str, Any], party_size: Optional[int]) -> str:
	amount = book_data.get("amount")
	network = book_data.get("network", "unknown")
	party_text = f" for {party_size} guest(s)" if party_size else ""
	return (
		"Great, I found a room and created your payment request.\n\n"
		f"Room: {book_payload.get('roomType')}\n"
		f"Stay: {format_date_human(book_payload.get('checkIn'))} to {format_date_human(book_payload.get('checkOut'))}{party_text}\n"
		f"Amount: {amount}\n"
		f"Network: {network}\n\n"
		"When you're ready, reply PAY and I'll send the payment transaction and finalize your booking."
	)


def format_book_intent_failure_message(status_code: int, data: Dict[str, Any]) -> str:
	error = str(data.get("error") or data.get("message") or "Unknown error")
	if status_code == 404 and "No available room" in error:
		return (
			"I couldn't find an available room for those dates and room type. "
			"Try a different room type or adjust the dates, and I'll check again."
		)
	if status_code == 502:
		return "I couldn't reach one of our booking systems just now. Please try again in a moment."
	return f"I couldn't create the booking request yet ({status_code}): {error}."


def format_confirm_success_message(confirm_data: Dict[str, Any], tx_hash: str) -> str:
	booking = confirm_data.get("booking", {}) if isinstance(confirm_data, dict) else {}
	room = confirm_data.get("room", {}) if isinstance(confirm_data, dict) else {}
	loyalty = confirm_data.get("loyalty", {}) if isinstance(confirm_data, dict) else {}

	booking_id = booking.get("id") or room.get("bookingId") or "N/A"
	room_type = booking.get("room_type") or room.get("roomType") or "your room"
	check_in = booking.get("check_in") or room.get("checkIn")
	check_out = booking.get("check_out") or room.get("checkOut")
	points = loyalty.get("points")

	lines = [
		"Your booking is confirmed.",
		f"Booking ID: {booking_id}",
		f"Room: {room_type}",
		f"Stay: {format_date_human(check_in)} to {format_date_human(check_out)}",
		f"Transaction: {tx_hash}",
	]
	if points is not None:
		lines.append(f"Loyalty: {points} points added.")
	else:
		lines.append("Loyalty: points update is queued and will be reflected shortly.")

	return "\n".join(lines)


def extract_slots_with_openai(text: str, current_slots: Dict[str, Optional[str]]) -> Dict[str, str]:
	required_keys = ["roomType", "checkIn", "checkOut", "customerEmail", "customerMobile"]
	fallback = fallback_extract_slots(text)

	if not openai_client:
		return fallback

	prompt = {
		"task": "Extract booking fields from user message.",
		"required_keys": ["roomType", "checkIn", "checkOut", "customerEmail", "customerMobile"],
		"format_rules": {
			"checkIn": "YYYY-MM-DD",
			"checkOut": "YYYY-MM-DD",
			"unknown_value": None,
		},
		"current_slots": current_slots,
		"user_message": text,
		"output_format": {"roomType": None, "checkIn": None, "checkOut": None, "customerEmail": None, "customerMobile": None},
	}

	try:
		response = openai_client.responses.create(
			model=OPENAI_MODEL,
			input=[
				{
					"role": "system",
					"content": "Return only valid JSON object with keys: roomType, checkIn, checkOut, customerEmail, customerMobile. Use null for unknown.",
				},
				{"role": "user", "content": json.dumps(prompt)},
			],
			temperature=0,
		)
		raw = response.output_text.strip()
		parsed = parse_json_text(raw)
		extracted = {
			k: v
			for k, v in parsed.items()
			if k in required_keys and v not in (None, "")
		}

		# If model output is malformed or empty, keep the bot reliable with regex fallback.
		if not extracted:
			return fallback

		for key, value in fallback.items():
			extracted.setdefault(key, value)

		return extracted
	except Exception:
		return fallback


def first_missing_slot(slots: Dict[str, Optional[str]]) -> Optional[str]:
	for key in ["roomType", "checkIn", "checkOut", "customerEmail", "customerMobile"]:
		if not slots.get(key):
			return key
	return None


def list_missing_slots(slots: Dict[str, Optional[str]]) -> list[str]:
	return [
		key
		for key in ["roomType", "checkIn", "checkOut", "customerEmail", "customerMobile"]
		if not slots.get(key)
	]


def missing_prompt(slot_name: str) -> str:
	prompts = {
		"roomType": "What room type do you want (e.g. Standard/Deluxe/Suite)?",
		"checkIn": "What is your check-in date? Use YYYY-MM-DD.",
		"checkOut": "What is your check-out date? Use YYYY-MM-DD.",
		"customerEmail": "What email should we use for the booking?",
		"customerMobile": "What mobile number should we use?",
	}
	return prompts.get(slot_name, "Please provide the missing booking detail.")


def slot_label(slot_name: str) -> str:
	labels = {
		"roomType": "room type",
		"checkIn": "check-in date",
		"checkOut": "check-out date",
		"customerEmail": "email",
		"customerMobile": "mobile number",
	}
	return labels.get(slot_name, slot_name)


def natural_slot_phrase(slot_name: str) -> str:
	phrases = {
		"roomType": "preferred room type",
		"checkIn": "check-in date",
		"checkOut": "check-out date",
		"customerEmail": "email address",
		"customerMobile": "mobile number",
	}
	return phrases.get(slot_name, slot_name)


def join_with_and(items: list[str]) -> str:
	if not items:
		return ""
	if len(items) == 1:
		return items[0]
	if len(items) == 2:
		return f"{items[0]} and {items[1]}"
	return ", ".join(items[:-1]) + f", and {items[-1]}"


def build_missing_details_message(missing: list[str]) -> str:
	phrases = [natural_slot_phrase(item) for item in missing]
	requested = join_with_and(phrases)

	if set(missing) == {"customerEmail", "customerMobile"}:
		return (
			"Great choice. To complete your reservation, may I have your email address and mobile number?\n"
			"Example: email test@example.com, mobile 81234567"
		)

	if len(missing) == 1:
		return f"Great, I can proceed. Could you share your {requested}?"

	return (
		f"Great, I can proceed. Could you share your {requested} so I can complete the booking?"
	)


def call_orchestrator_book(payload: Dict[str, Any]) -> requests.Response:
	return requests.post(ROOM_BOOK_ROUTE, json=payload, timeout=REQUEST_TIMEOUT)


def call_orchestrator_confirm(payload: Dict[str, Any]) -> requests.Response:
	return requests.post(ROOM_CONFIRM_ROUTE, json=payload, timeout=REQUEST_TIMEOUT)


def call_orchestrator_room_options() -> requests.Response:
	return requests.get(ROOM_OPTIONS_ROUTE, timeout=REQUEST_TIMEOUT)


def format_room_options_message(data: Dict[str, Any]) -> Optional[str]:
	options = data.get("roomOptions")
	currency = data.get("currency", "SGD")
	if not isinstance(options, list) or not options:
		return None

	lines = ["Available room options:"]
	for item in options:
		room_type = item.get("roomType", "Unknown")
		starting_cost = item.get("startingCost")
		available_rooms = item.get("availableRooms", 0)
		total_rooms = item.get("totalRooms", 0)

		if isinstance(starting_cost, (int, float)):
			price_text = f"from {currency} {starting_cost:.2f}/night"
		else:
			price_text = f"price unavailable ({currency})"

		lines.append(
			f"- {room_type}: {price_text} | available {available_rooms}/{total_rooms}"
		)

	lines.append("")
	lines.append("Now send your booking details: roomType, checkIn, checkOut, customerEmail, customerMobile.")
	return "\n".join(lines)


def should_retry_confirm(status_code: int, data: Dict[str, Any]) -> bool:
	if status_code < 400:
		return False

	settle = data.get("settle") if isinstance(data, dict) else None
	settle_status = ""
	if isinstance(settle, dict):
		settle_status = str(settle.get("status", "")).lower()

	# Retry only when receipt is likely not indexed/mined yet.
	return "invalid transaction receipt" in settle_status


def amount_to_base_units(amount_value: Any) -> int:
	return int(Decimal(str(amount_value)) * (Decimal(10) ** TOKEN_DECIMALS))


def send_token_payment(user_private_key: str, to_address: str, amount: Any) -> str:
	w3 = Web3(Web3.HTTPProvider(RPC_URL))
	if not w3.is_connected():
		raise RuntimeError("Could not connect to RPC")

	transfer_abi = [
		{
			"constant": False,
			"inputs": [
				{"name": "_to", "type": "address"},
				{"name": "_value", "type": "uint256"},
			],
			"name": "transfer",
			"outputs": [{"name": "", "type": "bool"}],
			"type": "function",
		}
	]

	sender_account = w3.eth.account.from_key(user_private_key)
	sender = Web3.to_checksum_address(sender_account.address)
	recipient = Web3.to_checksum_address(to_address)
	token = w3.eth.contract(address=Web3.to_checksum_address(TOKEN_CONTRACT), abi=transfer_abi)

	value_units = amount_to_base_units(amount)
	nonce = w3.eth.get_transaction_count(sender)

	tx = token.functions.transfer(recipient, value_units).build_transaction(
		{
			"from": sender,
			"nonce": nonce,
			"chainId": CHAIN_ID,
			"gas": 120000,
			"gasPrice": w3.eth.gas_price,
		}
	)

	signed = w3.eth.account.sign_transaction(tx, private_key=user_private_key)
	tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
	return w3.to_hex(tx_hash)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
	if not update.effective_user or not update.message:
		return
	state = get_session(update.effective_user.id)
	clear_booking_flow(state)
	await update.message.reply_text(
		"Booking assistant ready. Use /book to start. Use /setkey <private_key> to set your Web3 key for payment signing."
	)


async def setkey(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
	if not update.effective_user or not update.message:
		return
	state = get_session(update.effective_user.id)

	if not context.args:
		await update.message.reply_text("Usage: /setkey <hex_private_key>")
		return

	key = context.args[0].strip()
	if key.startswith("0x"):
		key = key[2:]
	if len(key) != 64:
		await update.message.reply_text("Invalid private key length.")
		return

	state.private_key = "0x" + key
	await update.message.reply_text("Private key stored for this bot session.")


async def book(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
	if not update.effective_user or not update.message:
		return
	state = get_session(update.effective_user.id)
	clear_booking_flow(state)
	state.collecting = True

	options_message = None
	try:
		options_resp = call_orchestrator_room_options()
		if options_resp.status_code < 400:
			options_data = options_resp.json()
			options_message = format_room_options_message(options_data)
	except Exception:
		options_message = None

	if options_message:
		await update.message.reply_text(
			"Sure, I can help with that.\n\n"
			f"{options_message}"
		)
	else:
		await update.message.reply_text(
			"Sure, I can help with that. Tell me your booking details naturally, for example: "
			"'I want a deluxe room from 12 April 2026 to 14 April 2026 for 2 guests, email: me@x.com, mobile 81234567'."
		)


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
	if not update.effective_user or not update.message or not update.message.text:
		return

	user_id = update.effective_user.id
	text = update.message.text.strip()
	state = get_session(user_id)
	party_size = extract_party_size(text)
	if party_size:
		state.party_size = party_size

	if state.awaiting_payment_confirm:
		lowered = text.lower()
		if lowered not in {"pay", "yes", "confirm", "y"}:
			await update.message.reply_text("Payment cancelled. You can restart using /book.")
			clear_booking_flow(state)
			return

		if not state.private_key:
			await update.message.reply_text(
				"No private key in session. Use /setkey <private_key> then send PAY again."
			)
			return

		pay_to = state.payment_info.get("payTo")
		amount = state.payment_info.get("amount")
		payment_intent_id = state.payment_info.get("paymentIntentId")
		hold_id = state.payment_info.get("holdId")

		if not pay_to or amount is None or not payment_intent_id or not hold_id:
			await update.message.reply_text("Missing payment context. Restart with /book.")
			clear_booking_flow(state)
			return

		try:
			tx_hash = send_token_payment(state.private_key, pay_to, amount)
		except Exception as exc:
			await update.message.reply_text(f"Payment transaction failed: {exc}")
			return

		confirm_payload = {
			"paymentIntentId": payment_intent_id,
			"tx_hash": tx_hash,
			"holdID": hold_id,
		}

		confirm_resp = None
		confirm_data: Dict[str, Any] = {}
		for attempt in range(1, CONFIRM_MAX_RETRIES + 1):
			try:
				confirm_resp = call_orchestrator_confirm(confirm_payload)
				confirm_data = confirm_resp.json()
			except Exception as exc:
				if attempt == CONFIRM_MAX_RETRIES:
					await update.message.reply_text(f"Confirm booking call failed: {exc}")
					return
				await asyncio.sleep(CONFIRM_RETRY_DELAY_SECONDS)
				continue

			if not should_retry_confirm(confirm_resp.status_code, confirm_data):
				break

			if attempt < CONFIRM_MAX_RETRIES:
				await asyncio.sleep(CONFIRM_RETRY_DELAY_SECONDS)

		if confirm_resp is None:
			await update.message.reply_text("Confirm booking call failed: no response received")
			return

		if confirm_resp.status_code >= 400:
			error_message = str(confirm_data.get("error") or confirm_data.get("message") or "Unknown error")
			await update.message.reply_text(
				f"I couldn't finalize the booking yet ({confirm_resp.status_code}): {error_message}."
			)
			return

		await update.message.reply_text(format_confirm_success_message(confirm_data, tx_hash))
		clear_booking_flow(state)
		return

	if not state.collecting:
		if detect_booking_intent(text):
			state.collecting = True
			await update.message.reply_text(
				"Great, I can help with that booking. I'll extract details from your message now."
			)
		else:
			await update.message.reply_text(
				"I can help you book rooms. Send something like: "
				"'Book a standard room from 12 April 2026 to 14 April 2026 for 2 guests, email test@example.com, mobile 81234567' "
				"or use /book."
			)
			return

	extracted = extract_slots_with_openai(text, state.slots)
	for key, value in extracted.items():
		if key in state.slots and value:
			if key == "roomType":
				state.slots[key] = normalize_room_type(str(value))
			else:
				state.slots[key] = str(value)

	missing = list_missing_slots(state.slots)
	if missing:
		await update.message.reply_text(build_missing_details_message(missing))
		return

	book_payload = {
		"roomType": state.slots["roomType"],
		"checkIn": state.slots["checkIn"],
		"checkOut": state.slots["checkOut"],
		"customerEmail": state.slots["customerEmail"],
		"customerMobile": state.slots["customerMobile"],
	}

	try:
		book_resp = call_orchestrator_book(book_payload)
		book_data = book_resp.json()
	except Exception as exc:
		await update.message.reply_text(f"Booking intent call failed: {exc}")
		return

	if book_resp.status_code >= 400:
		await update.message.reply_text(format_book_intent_failure_message(book_resp.status_code, book_data))
		return

	payment_intent_id = book_data.get("paymentIntentId")
	hold_id = book_data.get("holdId")
	pay_to = book_data.get("payTo")
	amount = book_data.get("amount")

	if not payment_intent_id or not hold_id or not pay_to or amount is None:
		await update.message.reply_text(
			"Booking step 1 succeeded but response is missing payment details needed for step 2."
		)
		return

	state.payment_info = {
		"paymentIntentId": payment_intent_id,
		"holdId": hold_id,
		"payTo": pay_to,
		"amount": amount,
		"network": book_data.get("network", "unknown"),
	}
	state.awaiting_payment_confirm = True

	await update.message.reply_text(
		format_book_intent_success_message(book_payload, book_data, state.party_size)
	)


def main() -> None:
	if not TELEGRAM_BOT_TOKEN:
		raise RuntimeError("Missing TELEGRAM_BOT_TOKEN environment variable")

	application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
	application.add_handler(CommandHandler("start", start))
	application.add_handler(CommandHandler("book", book))
	application.add_handler(CommandHandler("setkey", setkey))
	application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

	application.run_polling()


if __name__ == "__main__":
	main()
