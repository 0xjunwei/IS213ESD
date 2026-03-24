USE rooms;

CREATE TABLE IF NOT EXISTS rooms (
    id INT AUTO_INCREMENT PRIMARY KEY,
    roomID INT NOT NULL,
    roomType VARCHAR(255) NOT NULL,
    reservationDate TIMESTAMP NULL,
    costForTonight DOUBLE NOT NULL,
    status VARCHAR(50) NOT NULL,
    bookingId INT NULL,
    holdId VARCHAR(255) NULL,
    holdExpiry TIMESTAMP NULL,
    checkIn DATE NULL,
    checkOut DATE NULL
);

INSERT INTO rooms (roomID, roomType, reservationDate, costForTonight, status, bookingId, holdId, holdExpiry, checkIn, checkOut)
VALUES
(101, 'Standard', NULL, 120.00, 'available', NULL, NULL, NULL, NULL, NULL),
(102, 'Standard', NULL, 122.00, 'available', NULL, NULL, NULL, NULL, NULL),
(103, 'Standard', NULL, 124.00, 'available', NULL, NULL, NULL, NULL, NULL),
(104, 'Standard', NULL, 126.00, 'available', NULL, NULL, NULL, NULL, NULL),
(105, 'Standard', NULL, 128.00, 'available', NULL, NULL, NULL, NULL, NULL),
(106, 'Standard', NULL, 130.00, 'available', NULL, NULL, NULL, NULL, NULL),
(107, 'Standard', NULL, 132.00, 'available', NULL, NULL, NULL, NULL, NULL),
(108, 'Standard', NULL, 134.00, 'available', NULL, NULL, NULL, NULL, NULL),
(109, 'Standard', NULL, 136.00, 'available', NULL, NULL, NULL, NULL, NULL),
(110, 'Standard', NULL, 138.00, 'available', NULL, NULL, NULL, NULL, NULL),

(201, 'Deluxe', NULL, 180.00, 'available', NULL, NULL, NULL, NULL, NULL),
(202, 'Deluxe', NULL, 185.00, 'available', NULL, NULL, NULL, NULL, NULL),
(203, 'Deluxe', NULL, 190.00, 'available', NULL, NULL, NULL, NULL, NULL),
(204, 'Deluxe', NULL, 195.00, 'available', NULL, NULL, NULL, NULL, NULL),
(205, 'Deluxe', NULL, 200.00, 'available', NULL, NULL, NULL, NULL, NULL),
(206, 'Deluxe', NULL, 205.00, 'available', NULL, NULL, NULL, NULL, NULL),
(207, 'Deluxe', NULL, 210.00, 'available', NULL, NULL, NULL, NULL, NULL),

(301, 'Suite', NULL, 250.00, 'available', NULL, NULL, NULL, NULL, NULL),
(302, 'Suite', NULL, 260.00, 'available', NULL, NULL, NULL, NULL, NULL),
(303, 'Suite', NULL, 270.00, 'available', NULL, NULL, NULL, NULL, NULL),
(304, 'Suite', NULL, 280.00, 'available', NULL, NULL, NULL, NULL, NULL),
(305, 'Suite', NULL, 290.00, 'available', NULL, NULL, NULL, NULL, NULL);