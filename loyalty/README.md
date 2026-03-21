# 🚀 Loyalty Service (Event-Driven)

## 📌 Overview
This service processes booking events asynchronously using **Redis Streams** and sends loyalty updates to **OutSystems**.

---

## ▶️ How to Run

### 1. Navigate to project folder
```bash
cd loyalty-service/loyalty
docker-compose up --build

## ▶️ How to Test
docker exec -it redis redis-cli

1.Booking paid 
XADD booking_stream * data '{"event":"booking_paid","email":"test@example.com","bookingId":"B001","amount":100}'

2.Booking Cancelled 
XADD booking_stream * data '{"event":"booking_cancelled","email":"test@example.com","bookingId":"B001","amount":100}'

3.Booking Modified 
XADD booking_stream * data '{"event":"booking_modified","email":"test@example.com","bookingId":"B001","old_amount":100,"new_amount":200}'