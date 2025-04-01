SHOW DATABASES;
#DROP DATABASE dw;
#CREATE DATABASE dw;

USE dw;


SHOW TABLES;

SELECT * FROM arrest;
SELECT * FROM commercial_info;
SELECT * FROM driver;
SELECT * FROM incident_details;
SELECT * FROM traffic_stop;
SELECT * FROM vehicle;
SELECT * FROM violation;

DROP TABLE IF EXISTS officer;


CREATE TABLE officer (
    id INTEGER PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    badge_number INTEGER NOT NULL UNIQUE,
    rank_position VARCHAR(50) NOT NULL,
    traffic_stop_id INTEGER NOT NULL,
    CONSTRAINT traffic_officer_fk FOREIGN KEY (traffic_stop_id) REFERENCES traffic_stop(id) 
        
);

INSERT INTO officer (name, last_name, badge_number, rank_position, traffic_stop_id) VALUES
('John', 'Jonson', 12345, 'Sergeant', 1),
('Jane', 'Smith', 67890, 'Lieutenant', 2),
('Mike', 'Johnson', 11223, 'Officer', 3),
('Emily', 'Davis', 44556, 'Captain', 4),
('Chris', 'Wilson', 78901, 'Detective', 5),
('David', 'Brown', 23456, 'Sergeant', 6),
('Sarah', 'Miller', 34567, 'Lieutenant', 7),
('Robert', 'Taylor', 45678, 'Officer', 8),
('Laura', 'Anderson', 56789, 'Captain', 9),
('James', 'Thomas', 67891, 'Detective', 10),
('Emma', 'Martinez', 78912, 'Sergeant', 11),
('Daniel', 'Harris', 89123, 'Lieutenant', 12),
('Sophia', 'Clark', 91234, 'Officer', 13),
('Matthew', 'Lewis', 10234, 'Captain', 14),
('Olivia', 'Walker', 11245, 'Detective', 15),
('Ryan', 'Hall', 12256, 'Sergeant', 16),
('Ava', 'Allen', 13267, 'Lieutenant', 17),
('Brandon', 'Young', 14278, 'Officer', 18),
('Ella', 'King', 15289, 'Captain', 19),
('Nathan', 'Wright', 16290, 'Detective', 20);


SELECT * FROM officer;

/* Ručna provjera unosa podataka u bazu podataka
   Output selecta mora u potpunosti odgovarati CSV podacima iz datoteke */

SELECT 
    ts.date_of_stop AS 'date_of_stop',
    ts.time_of_stop AS 'time_of_stop',
    ts.agency AS 'agency',
    ts.subagency AS 'subagency',
    ts.description AS 'description',
    ts.location AS 'location',
    ts.latitude AS 'latitude',
    ts.longitude AS 'longitude',
    ts.geolocation AS 'geolocation',
    id.accident AS 'accident',
    id.belts AS 'belts',
    id.personal_injury AS 'personal_injury',
    id.property_damage AS 'property_damage',
    id.fatal AS 'fatal',
    id.alcohol AS 'alcohol',
    id.work_zone AS 'work_zone',
    ts.state AS 'state',
    v.vehicletype AS 'vehicle_type',
    v.year AS 'vehicle_year',
    v.make AS 'vehicle_make',
    v.model AS 'vehicle_model',
    v.color AS 'vehicle_color',
    vi.violation_type AS 'violation_type',
    vi.charge AS 'charge',
    vi.article AS 'article',
    vi.contributed_to_accident AS 'contributed_to_accident',
    d.race AS 'driver_race',
    d.gender AS 'driver_gender',
    d.driver_city AS 'driver_city',
    d.driver_state AS 'driver_state',
    d.dl_state AS 'driver_license_state',
    a.arrest_type AS 'arrest_type',
    ci.commercial_license AS 'commercial_license',
    ci.hazmat AS 'hazmat',
    ci.commercial_vehicle AS 'commercial_vehicle',
    o.name AS 'officer_name',
    o.last_name AS 'officer_last_name',
    o.badge_number AS 'officer_badge_number',
    o.rank_position AS 'officer_rank'
FROM 
    traffic_stop ts
JOIN 
    incident_details id ON ts.id = id.traffic_stop_id
JOIN 
    vehicle v ON ts.id = v.traffic_stop_id
JOIN 
    violation vi ON ts.id = vi.traffic_stop_id
JOIN 
    driver d ON ts.id = d.traffic_stop_id
JOIN 
    arrest a ON ts.id = a.traffic_stop_id
JOIN 
    commercial_info ci ON ts.id = ci.traffic_stop_id
LEFT JOIN 
    officer o ON ts.id = o.traffic_stop_id
ORDER BY 
    ts.id ASC;





