# 🚀 Loyalty Service (Event-Driven with Redis Streams)

## 📌 Overview
This service handles loyalty point updates using an **event-driven architecture** powered by **Redis Streams**.

Instead of direct API calls, booking-related events are processed **asynchronously**, improving scalability and reliability.

## 🧱 Architecture

Booking Service / Orchestrator  
→ Redis Stream (`booking_stream`)  
→ Consumer Group (Loyalty Consumer)  
→ Loyalty Processing  
→ OutSystems API  

⚠️ Failed events → `dead_letter_stream`  
→ Analytics API  
→ Flask Dashboard (Monitoring & Insights)

---

## ✨ Key Features

- **Event-driven processing** (decoupled from booking service)
- **Redis Streams + Consumer Groups**
- **Dead Letter Stream for failed events**
- **Analytics Dashboard for monitoring failures**
- **OutSystems integration for loyalty points**

---

## ▶️ How to Run

### 1️⃣ Navigate to project folder
```bash
cd loyalty-service/loyalty
docker-compose up --build

How to test redis
docker exec -it loyalty-redis-1 redis-cli

1.Booking paid
XADD booking_stream * data '{"event":"booking_paid","email":"test@example.com","bookingId":"B001","amount":100}'
2.Booking cancelled
XADD booking_stream * data '{"event":"booking_cancelled","email":"test@example.com","bookingId":"B001","amount":100}'
3.Booking Modified 
XADD booking_stream * data '{"event":"booking_modified","email":"test@example.com","bookingId":"B001","old_amount":100,"new_amount":200}'