from flask import Flask, render_template
import redis
import json
import os

app = Flask(__name__)

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
redis_client = redis.Redis(
    host=REDIS_HOST,
    port=6379,
    decode_responses=True,
)

DEAD_STREAM = "dead_letter_stream"

@app.route("/health")
def health():
    return {"status": "ok"}, 200
# =========================
# 🔥 AUTO INSIGHT FUNCTION
# =========================
def generate_insights(stats):
    insights = []

    total = stats["total_failures"]

    if total == 0:
        insights.append("✅ No failures detected. System is healthy.")
        return insights

    # Most common error
    if stats.get("by_error"):
        top_error = max(stats["by_error"], key=stats["by_error"].get)
        error_count = stats["by_error"][top_error]
        percentage = round((error_count / total) * 100, 1)

        insights.append(f"⚠️ Most failures are caused by '{top_error}' ({percentage}%)")

    # Most common event
    if stats.get("by_event"):
        top_event = max(stats["by_event"], key=stats["by_event"].get)
        insights.append(f"📌 Most affected event: {top_event}")

    # System health indicator
    if total > 10:
        insights.append("🚨 High failure rate detected. Immediate attention needed.")
    elif total > 3:
        insights.append("⚠️ Moderate failure rate. Monitor closely.")
    else:
        insights.append("✅ Low failure rate. System stable.")

    return insights


# =========================
# ROUTE
# =========================
@app.route("/")
def dashboard():
    messages = redis_client.xrange(DEAD_STREAM, "-", "+")

    stats = {
        "total_failures": len(messages),
        "by_event": {},
        "by_error": {},
        "events": [],
    }

    for message_id, msg in messages:
        try:
            raw_data = msg.get("data", "{}")
            data = json.loads(raw_data)

            event = data.get("event", "unknown")
            error = msg.get("error", "unknown")

            stats["by_event"][event] = stats["by_event"].get(event, 0) + 1
            stats["by_error"][error] = stats["by_error"].get(error, 0) + 1

            stats["events"].append(
                {"id": message_id, "event": event, "error": error, "data": data}
            )

        except Exception as e:
            stats["events"].append({"id": message_id, "error": str(e), "data": msg})

    # ✅ ADD INSIGHTS HERE
    insights = generate_insights(stats)

    return render_template(
        "dashboard.html", stats=stats, insights=insights  # 🔥 PASS TO FRONTEND
    )


# =========================
# ENTRY POINT
# =========================
if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5001, debug=True)
