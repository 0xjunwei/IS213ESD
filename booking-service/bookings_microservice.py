import environ
import time
import mysql.connector
from mysql.connector import Error
from flask import Flask, request, jsonify

app = Flask(__name__)

# Initialise environment reader
env = environ.Env()

# Load the .env file
environ.Env.read_env()

DB_HOST = env("DB_HOST")
DB_PORT = env.int("DB_PORT")
DB_NAME = env("DB_NAME")
DB_USER = env("DB_USER")
DB_PASSWORD = env("DB_PASSWORD")

def get_connection():
    return mysql.connector.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

def init_db():
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS bookings (
        id INT PRIMARY KEY,
        room_id INT NOT NULL,
        room_type VARCHAR(100) NOT NULL,
        customer_email VARCHAR(255) NOT NULL,
        customer_mobile VARCHAR(50) NOT NULL,
        check_in TIMESTAMP NOT NULL,
        check_out TIMESTAMP NOT NULL,
        amount_spent INT NOT NULL,
        hold_id VARCHAR(100),
        legacy_hold_id VARCHAR(100)
    )
    """
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(create_table_sql)

    conn.commit()
    cursor.close()
    conn.close()

def wait_for_db(max_retries=20, delay=3):
    for attempt in range(max_retries):
        try:
            conn = get_connection()
            conn.close()
            print("Connected to MySQL.")
            return
        except Error as e:
            print(f"MySQL not ready yet ({attempt + 1}/{max_retries}): {e}")
            time.sleep(delay)
    raise RuntimeError("Could not connect to MySQL after multiple retries.")


#get the bookings
@app.get("/bookings/<int:booking_id>")
def get_booking(booking_id):
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM bookings WHERE id = %s", (booking_id,))
        row = cursor.fetchone()
        cursor.close()
        conn.close()

        if not row:
            return jsonify({"message": "Booking not found"}), 404

        return jsonify(row), 200
    except Error as e:
        return jsonify({"message": "Failed to fetch booking", "error": str(e)}), 500


@app.get("/bookings")
def list_bookings():
    """
    List bookings, optionally filtered by customerEmail query parameter.
    """
    customer_email = (request.args.get("customerEmail") or "").strip()

    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        if customer_email:
            cursor.execute(
                "SELECT * FROM bookings WHERE customer_email = %s ORDER BY id DESC",
                (customer_email,),
            )
        else:
            cursor.execute("SELECT * FROM bookings ORDER BY id DESC")

        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        return jsonify(rows), 200
    except Error as e:
        return jsonify({"message": "Failed to fetch bookings", "error": str(e)}), 500


#allows the creation of a booking
@app.post("/bookings/create")
def create_booking():
    try:
        data = request.get_json()

        required_fields = [
            "id",
            "roomID",
            "roomType",
            "customerEmail",
            "customerMobile",
            "checkIn",
            "amountSpent",
            "checkOut",
        ]

        for field in required_fields:
            if field not in data or data[field] in [None, ""]:
                return jsonify({"message": f"Missing required field: {field}"}), 400

        if data["checkOut"] <= data["checkIn"]:
            return jsonify({"message": "checkOut must be later than checkIn"}), 400

        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        insert_sql = """
        INSERT INTO bookings
        (
            id,
            room_id,
            room_type,
            customer_email,
            customer_mobile,
            check_in,
            check_out,
            amount_spent,
            hold_id,
            legacy_hold_id
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        values = (
            data["id"],
            data["roomID"],
            data["roomType"],
            data["customerEmail"],
            data["customerMobile"],
            data["checkIn"],
            data["checkOut"],
            data["amountSpent"],
            data["holdId"],
            data.get("legacyHoldId")
        )

        cursor.execute(insert_sql, values)
        conn.commit()

        cursor.execute("SELECT * FROM bookings WHERE id = %s", (data["id"],))
        created = cursor.fetchone()

        cursor.close()
        conn.close()

        return jsonify(created), 201

    except mysql.connector.IntegrityError:
        return jsonify({"message": "Booking ID already exists"}), 409
    except Error as e:
        return jsonify({"message": "Failed to create booking", "error": str(e)}), 500
    

#deletes booking
@app.delete("/bookings/<int:booking_id>")
def delete_booking(booking_id):
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        cursor.execute("SELECT * FROM bookings WHERE id = %s", (booking_id,))
        existing = cursor.fetchone()

        if not existing:
            cursor.close()
            conn.close()
            return jsonify({"message": "Booking not found"}), 404

        cursor.execute("DELETE FROM bookings WHERE id = %s", (booking_id,))
        conn.commit()

        cursor.close()
        conn.close()

        return jsonify({
            "message": "Booking deleted successfully",
            "booking": existing
        }), 200

    except Error as e:
        return jsonify({"message": "Failed to delete booking", "error": str(e)}), 500

if __name__ == "__main__":
    wait_for_db()
    init_db()
    app.run(host="0.0.0.0", port=5000, debug=False)

#updates booking
@app.post("/bookings/update")
def update_booking():
    try:
        data = request.get_json()

        if not data:
            return jsonify({"message": "Invalid or missing JSON body"}), 400

        old_hold_id = data.get("oldHoldId")
        new_hold_id = data.get("newHoldId")
        new_details = data.get("newDetails", {})

        if not old_hold_id:
            return jsonify({"message": "Missing required field: oldHoldId"}), 400

        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        # Check whether booking with oldHoldId exists
        cursor.execute("SELECT * FROM bookings WHERE hold_id = %s", (old_hold_id,))
        existing = cursor.fetchone()

        if not existing:
            cursor.close()
            conn.close()
            return jsonify({"message": "Booking with given oldHoldId not found"}), 404

        # Build update fields dynamically
        update_fields = []
        values = []

        # Update hold_id if newHoldId is provided
        if new_hold_id is not None:
            update_fields.append("hold_id = %s")
            values.append(new_hold_id)

        # Map incoming JSON names to DB column names
        field_mapping = {
            "roomID": "room_id",
            "roomType": "room_type",
            "customerEmail": "customer_email",
            "customerMobile": "customer_mobile",
            "checkIn": "check_in",
            "checkOut": "check_out"
        }

        for json_field, db_field in field_mapping.items():
            if json_field in new_details:
                update_fields.append(f"{db_field} = %s")
                values.append(new_details[json_field])

        if not update_fields:
            cursor.close()
            conn.close()
            return jsonify({"message": "No fields provided to update"}), 400

        # checks if for dates are correct 
        check_in = new_details.get("checkIn", existing["check_in"])
        check_out = new_details.get("checkOut", existing["check_out"])

        if str(check_out) <= str(check_in):
            cursor.close()
            conn.close()
            return jsonify({"message": "checkOut must be later than checkIn"}), 400

        update_sql = f"""
        UPDATE bookings
        SET {', '.join(update_fields)}
        WHERE hold_id = %s
        """
        values.append(old_hold_id)

        cursor.execute(update_sql, tuple(values))
        conn.commit()

        # Fetch updated booking to showcase after updating
        lookup_hold_id = new_hold_id if new_hold_id is not None else old_hold_id
        cursor.execute("SELECT * FROM bookings WHERE hold_id = %s", (lookup_hold_id,))
        updated = cursor.fetchone()

        cursor.close()
        conn.close()

        return jsonify({
            "message": "Booking updated successfully",
            "booking": updated
        }), 200

    except Error as e:
        return jsonify({"message": "Failed to update booking", "error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
