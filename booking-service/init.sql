CREATE TABLE IF NOT EXISTS bookings (
    id INT PRIMARY KEY,
    room_id INT NOT NULL,
    room_type VARCHAR(100) NOT NULL,
    customer_email VARCHAR(255) NOT NULL,
    customer_mobile VARCHAR(50) NOT NULL,
    check_in TIMESTAMP NOT NULL,
    check_out TIMESTAMP NOT NULL,
    amount_spent INT NOT NULL,
    hold_id VARCHAR(100) NOT NULL
    
);