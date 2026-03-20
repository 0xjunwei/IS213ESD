import environ
import time
import mysql.connector
from mysql.connector import Error
from flask import Flask, request, jsonify
import uuid
from datetime import datetime, timedelta


app = Flask(__name__)
env = environ.Env()
environ.Env.read_env()


# ================================ DB START================================ #

DB_HOST = env("DB_HOST", default="localhost")
DB_PORT = env.int("DB_PORT", default=3306)
DB_NAME = env("DB_NAME", default="rooms")
DB_USER = env("DB_USER", default="root")
DB_PASSWORD = env("DB_PASSWORD", default="root")







def get_connection():
    return mysql.connector.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )


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

# ================================ DB END ================================ #

# ================================ ENDPOINT START ================================ #

@app.get("/health")
def health():
    return jsonify({"status": "healthy"}), 200

@app.get("/rooms")
def get_all_rooms():
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        cursor.execute("SELECT * FROM rooms")
        rooms = cursor.fetchall()

        cursor.close()
        conn.close()

        return jsonify(rooms), 200

    except Error as e:
        return jsonify({"message": "Database error", "error": str(e)}), 500
    except Exception as e:
        return jsonify({"message": "Unexpected error", "error": str(e)}), 500
    

@app.route("/rooms/holds", methods=["POST"])
def create_hold():
    data = request.get_json()

    room_type = data.get("roomType")
    check_in = data.get("checkIn")
    check_out = data.get("checkOut")

    # if not room_type:
    #     return jsonify({"error": "roomType required"}), 400
    
    if not room_type or not check_in or not check_out:
        return jsonify({"error": "roomType, checkIn and checkOut are required"}), 400

    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    try:
        
        # EXPIRED HOLD CHECKS DONE WHENEVER SOMEONE CALLS THIS
        cursor.execute("""
        UPDATE rooms
        SET status = 'available',
            holdId = NULL,
            holdExpiry = NULL,
            checkIn = NULL,
            checkOut = NULL
        WHERE status = 'held' AND holdExpiry < NOW()
        """)
        conn.commit()
        
        # 1. find available room
        cursor.execute("""
        SELECT * FROM rooms
        WHERE roomType = %s
        AND (
                status = 'available'
                OR (
                    status = 'held' AND holdExpiry < NOW()
                )
            )
        AND (
                checkIn IS NULL
                OR checkOut IS NULL
                OR checkOut <= %s
                OR checkIn >= %s
            )
        LIMIT 1
        """, (room_type, check_in, check_out))

        room = cursor.fetchone()

        if not room:
            return jsonify({"error": "No available room"}), 404

        # 2. generate holdId
        hold_id = str(uuid.uuid4())

        # 3. set expiry (5 minutes)
        expiry_time = datetime.now() + timedelta(minutes=5)
        check_in_date = datetime.strptime(check_in, "%Y-%m-%d")
        check_out_date = datetime.strptime(check_out, "%Y-%m-%d")
        nights = (check_out_date - check_in_date).days

        if nights <= 0:
            return jsonify({"error": "checkOut must be after checkIn"}), 400

        cost = nights * 100

        # 4. update room
        cursor.execute("""
        UPDATE rooms
        SET status = 'held',
            holdId = %s,
            holdExpiry = %s,
            checkIn = %s,
            checkOut = %s
        WHERE roomID = %s
        """, (hold_id, expiry_time, check_in, check_out, room["roomID"]))

        conn.commit()

        return jsonify({
            "roomID": room["roomID"],
            "holdId": hold_id,
            "roomType": room_type,
            "checkIn": check_in,
            "checkOut": check_out,
            "amount": cost,
            "timeToExpire": expiry_time.strftime("%Y-%m-%d %H:%M:%S")
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        cursor.close()
        conn.close()
    

@app.post("/rooms/update")
def update_room():
    try:
        data = request.get_json()

        if not data:
            return jsonify({"message": "Missing JSON body"}), 400

        if "holdId" not in data:
            return jsonify({"message": "Missing field: holdId"}), 400

        hold_id = data["holdId"]
        status = data.get("status")
        booking_id = data.get("bookingId")
        reservation_date = data.get("reservationDate")

        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        cursor.execute("SELECT * FROM rooms WHERE holdId = %s", (hold_id,))
        existing_room = cursor.fetchone()

        if not existing_room:
            cursor.close()
            conn.close()
            return jsonify({"message": "Room not found for given holdId"}), 404

        update_fields = []
        values = []

        if status is not None:
            update_fields.append("status = %s")
            values.append(status)

        if booking_id is not None:
            update_fields.append("bookingId = %s")
            values.append(booking_id)

        if reservation_date is not None:
            update_fields.append("reservationDate = %s")
            values.append(reservation_date)

        if not update_fields:
            cursor.close()
            conn.close()
            return jsonify({"message": "No fields provided to update"}), 400

        values.append(hold_id)

        sql = f"""
            UPDATE rooms
            SET {', '.join(update_fields)}
            WHERE holdId = %s
        """
        cursor.execute(sql, tuple(values))
        conn.commit()

        cursor.execute("SELECT * FROM rooms WHERE holdId = %s", (hold_id,))
        updated_room = cursor.fetchone()

        cursor.close()
        conn.close()

        return jsonify(updated_room), 200

    except Error as e:
        return jsonify({"message": "Database error", "error": str(e)}), 500
    except Exception as e:
        return jsonify({"message": "Unexpected error", "error": str(e)}), 500


# @app.post("/rooms/update")
# def update_room():
#     try:
#         data = request.get_json()

#         if not data:
#             return jsonify({"message": "Missing JSON body"}), 400

#         if "roomID" not in data:
#             return jsonify({"message": "Missing field: roomID"}), 400

#         room_id = data["roomID"]
#         room_type = data.get("roomType")
#         cost_for_tonight = data.get("costForTonight")
#         status = data.get("status")

#         conn = get_connection()
#         cursor = conn.cursor(dictionary=True)

#         cursor.execute("SELECT * FROM rooms WHERE roomID = %s", (room_id,))
#         existing_room = cursor.fetchone()

#         if not existing_room:
#             cursor.close()
#             conn.close()
#             return jsonify({"message": "Room not found"}), 404

#         update_fields = []
#         values = []

#         if room_type is not None:
#             update_fields.append("roomType = %s")
#             values.append(room_type)

#         if cost_for_tonight is not None:
#             update_fields.append("costForTonight = %s")
#             values.append(cost_for_tonight)

#         if status is not None:
#             update_fields.append("status = %s")
#             values.append(status)

#         if not update_fields:
#             cursor.close()
#             conn.close()
#             return jsonify({"message": "No fields provided to update"}), 400

#         values.append(room_id)

#         sql = f"""
#             UPDATE rooms
#             SET {', '.join(update_fields)}
#             WHERE roomID = %s
#         """
#         cursor.execute(sql, tuple(values))
#         conn.commit()

#         cursor.execute("SELECT * FROM rooms WHERE roomID = %s", (room_id,))
#         updated_room = cursor.fetchone()

#         cursor.close()
#         conn.close()

#         return jsonify(updated_room), 200

#     except Error as e:
#         return jsonify({"message": "Database error", "error": str(e)}), 500
#     except Exception as e:
#         return jsonify({"message": "Unexpected error", "error": str(e)}), 500



# @app.post("/rooms/update-reservation")
# def update_reservation():
#     try:
#         data = request.get_json()

#         if not data:
#             return jsonify({"message": "Missing JSON body"}), 400

#         required_fields = ["roomID", "reservationDate", "status"]
#         for field in required_fields:
#             if field not in data:
#                 return jsonify({"message": f"Missing field: {field}"}), 400

#         room_id = data["roomID"]
#         reservation_date = data["reservationDate"]
#         status = data["status"]
#         booking_id = data.get("bookingId")
#         hold_id = data.get("holdId")

#         conn = get_connection()
#         cursor = conn.cursor(dictionary=True)

#         cursor.execute("SELECT * FROM rooms WHERE roomID = %s", (room_id,))
#         existing_room = cursor.fetchone()

#         if not existing_room:
#             cursor.close()
#             conn.close()
#             return jsonify({"message": "Room not found"}), 404

#         cursor.execute("""
#             UPDATE rooms
#             SET reservationDate = %s,
#                 status = %s,
#                 bookingId = %s,
#                 holdId = %s
#             WHERE roomID = %s
#         """, (reservation_date, status, booking_id, hold_id, room_id))

#         conn.commit()

#         cursor.execute("SELECT * FROM rooms WHERE roomID = %s", (room_id,))
#         updated_room = cursor.fetchone()

#         cursor.close()
#         conn.close()

#         return jsonify(updated_room), 200

#     except Error as e:
#         return jsonify({"message": "Database error", "error": str(e)}), 500
#     except Exception as e:
#         return jsonify({"message": "Unexpected error", "error": str(e)}), 500
    

# ================================ ENDPOINT END ================================ #
    
if __name__ == "__main__":
    wait_for_db()
    app.run(host="0.0.0.0", port=5003, debug=True)
    