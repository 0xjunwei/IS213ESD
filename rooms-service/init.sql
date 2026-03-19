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
    holdExpiry TIMESTAMP NULL
);

INSERT INTO rooms (roomID, roomType, reservationDate, costForTonight, status, bookingId, holdId, holdExpiry)
VALUES
-- (101, 'Standard', NULL, 120.00, 'available', NULL, NULL),
-- (102, 'Deluxe', NULL, 180.00, 'available', NULL, NULL),
-- (103, 'Suite', NULL, 250.00, 'maintenance', NULL, NULL);


(101, 'Standard', NULL, 120.00, 'available', NULL, NULL, NULL),
(102, 'Standard', NULL, 125.00, 'available', NULL, NULL, NULL),
(103, 'Standard', NULL, 130.00, 'available', NULL, NULL, NULL),
(201, 'Deluxe', NULL, 180.00, 'available', NULL, NULL, NULL),
(202, 'Deluxe', NULL, 190.00, 'available', NULL, NULL, NULL),
(203, 'Deluxe', NULL, 195.00, 'maintenance', NULL, NULL, NULL),
(301, 'Suite', NULL, 250.00, 'available', NULL, NULL, NULL),
(302, 'Suite', NULL, 270.00, 'available', NULL, NULL, NULL),
(303, 'Suite', NULL, 300.00, 'held', NULL, 'existing-hold-303', '2026-03-20 23:59:59');