# IS213ESD

Initial Set Up guide due to design the access of each microservice is internalized using Zerotier VPN (thus they be called using Private IP), assuming you are not I will be providing host.docker.internal as template:

Orchestration.py .env file:
```
AUTH_SERVICE=http://host.docker.internal:5000
PAYMENT_SERVICE=http://host.docker.internal:8080
FORWARD_TIMEOUT=8
BOOKINGS_SERVICE=http://host.docker.internal:5001
LOYALTY_SERVICE=http://host.docker.internal:5002
ROOMS_SERVICE=http://host.docker.internal:5003
REDIS_HOST=host.docker.internal
REDIS_PORT=6379
LOYALTY_STREAM=booking_stream

```

Booking Service .env file:
```
DB_HOST=db
DB_PORT=3306
DB_NAME=bookings_db
DB_USER={redacted}
DB_PASSWORD={redacted}
```

Loyalty Service .env file:
```
REDIS_HOST=redis
REDIS_PORT=6379

OUTSYSTEMS_URL={redacted}
```

Notification Wrapper .env file:
```
REDIS_HOST=redis
REDIS_PORT=6379
STREAM_NAME=email_stream
GROUP_NAME=email_group
CONSUMER_NAME=consumer_1

RESEND_API_KEY={redacted}
RESEND_FROM_EMAIL={redacted}
```

telebot .env file:
```
# Required
TELEGRAM_BOT_TOKEN={redacted}
OPENAI_API_KEY={redacted}
# Optional
OPENAI_MODEL=gpt-4o-mini
ORCHESTRATOR_BASE={redacted}
RPC_URL=https://sepolia.base.org
TOKEN_CONTRACT=0xbbD47B1eAdb7513a08c26C68E2669f4FE3B7Eae7
TOKEN_DECIMALS=6
CHAIN_ID=84532
REQUEST_TIMEOUT=20
```

Rooms Service creds was hard-coded into the docker-compose.yml, as it is internalized I have assessed the risk to be fine, would not overwrite others work based on this risk assessment.

## Microservices Coordinated
The Orchestration Service integrates with:
1. **Auth Service** (port 5000): User authentication and authorization
2. **Payment Service** (port 8080): Payment processing and refunds
3. **Bookings Service** (port 5001): Booking creation and management
4. **Rooms Service** (port 5003): Room availability and holds
5. **Loyalty Service** (via Redis): Loyalty points tracking
6. **Notification Service** (via Redis): Email notifications

## Prerequisites
Before running the Orchestration Service, ensure:
- **Docker** (version 20.10+) and **Docker Compose** (version 1.29+)
- **Python** 3.11+ (if running without Docker)
- **Redis** (version 6.0+ for session and event management)
- All dependent microservices running (Auth, Payment, Bookings, Rooms)
- Network connectivity between services

## Installation, the nature of installing each microservice would be similar, thus please repeat the following code in their respective folders

### Using Docker Compose
1. Navigate to the orchestration service directory / any other service folder **(Please start with other services and start Orchestrator last)**:
   All services are Atomic except for Orchestration, which is a composite additionally it acts like an API Gateway thus it should be the last to be started.
   ```bash
   cd orchestration
   ```

2. Build and start the orchestrator:
   ```bash
   docker-compose up -d
   ```
   This will start the Orchestration Service and set up health checks.

3. Verify the service is running:
   ```bash
   docker ps -a
   ```
   Expected response: `Seeing the service is healthy within the Docker List`

4. To view logs:
   ```bash
   docker-compose logs -f {Service Name}
   ```

5. To stop the service:
   ```bash
   docker-compose down
   ```
