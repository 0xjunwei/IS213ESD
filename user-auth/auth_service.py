import os
import time
import secrets
import psycopg2
import psycopg2.extras

from flask import Flask, request, jsonify
from werkzeug.security import generate_password_hash, check_password_hash

app = Flask(__name__)

DB_HOST = os.getenv("DB_HOST", "db")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "authdb")
DB_USER = os.getenv("DB_USER", "authuser")
DB_PASSWORD = os.getenv("DB_PASSWORD", "authpass")


def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )


def wait_for_db(max_retries=10, delay=3):
    for attempt in range(1, max_retries + 1):
        try:
            conn = get_db_connection()
            conn.close()
            print("Database is ready.")
            return
        except Exception as e:
            print(f"Database not ready yet ({attempt}/{max_retries}): {e}")
            time.sleep(delay)

    raise Exception("Could not connect to database.")


def init_db():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            userId SERIAL PRIMARY KEY,
            username VARCHAR(80) UNIQUE NOT NULL,
            password VARCHAR(255) NOT NULL,
            role VARCHAR(50) NOT NULL DEFAULT 'normal'
        )
    """)

    conn.commit()
    cur.close()
    conn.close()


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200


@app.route("/auth/register", methods=["POST"])
def register():
    data = request.get_json(silent=True) or {}

    username = str(data.get("username", "")).strip()
    password = str(data.get("password", "")).strip()

    if not username or not password:
        return jsonify({"error": "username and password are required"}), 400

    hashed_password = generate_password_hash(password)

    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    try:
        cur.execute(
            """
            INSERT INTO users (username, password, role)
            VALUES (%s, %s, %s)
            RETURNING userId, username, role
            """,
            (username, hashed_password, "normal")
        )

        user = cur.fetchone()
        conn.commit()

        return jsonify({
            "message": "user registered successfully",
            "user": {
                "userId": user["userid"] if "userid" in user else user["userId"],
                "username": user["username"],
                "role": user["role"]
            }
        }), 201

    except psycopg2.Error:
        conn.rollback()
        return jsonify({"error": "username already exists"}), 409

    finally:
        cur.close()
        conn.close()


@app.route("/auth/login", methods=["POST"])
def login():
    data = request.get_json(silent=True) or {}

    username = str(data.get("username", "")).strip()
    password = str(data.get("password", "")).strip()

    if not username or not password:
        return jsonify({"error": "username and password are required"}), 400

    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute(
        """
        SELECT userId, username, password, role
        FROM users
        WHERE username = %s
        """,
        (username,)
    )

    user = cur.fetchone()

    cur.close()
    conn.close()

    if not user:
        return jsonify({"error": "invalid username or password"}), 401

    stored_hash = user["password"]
    if not check_password_hash(stored_hash, password):
        return jsonify({"error": "invalid username or password"}), 401

    session_token = secrets.token_urlsafe(32)

    return jsonify({
        "message": "login successful",
        "token": session_token,
        "user": {
            "userId": user["userid"] if "userid" in user else user["userId"],
            "username": user["username"],
            "role": user["role"]
        }
    }), 200


if __name__ == "__main__":
    wait_for_db()
    init_db()
    app.run(host="0.0.0.0", port=5000)
