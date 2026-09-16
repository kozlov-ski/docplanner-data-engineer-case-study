-- Slow-moving dimensions. Seeded automatically on first `docker compose up`.

CREATE TABLE zones (
    zone_id INT PRIMARY KEY,
    city    TEXT NOT NULL,
    name    TEXT NOT NULL
);

INSERT INTO zones (zone_id, city, name) VALUES
    (1, 'Warsaw', 'Srodmiescie'),
    (2, 'Warsaw', 'Mokotow'),
    (3, 'Warsaw', 'Praga'),
    (4, 'Krakow', 'Stare Miasto'),
    (5, 'Krakow', 'Podgorze'),
    (6, 'Gdansk', 'Wrzeszcz');

CREATE TABLE restaurants (
    restaurant_id  INT PRIMARY KEY,
    name           TEXT NOT NULL,
    zone_id        INT NOT NULL REFERENCES zones(zone_id),
    cuisine        TEXT NOT NULL,
    tier           TEXT NOT NULL,
    commission_pct NUMERIC(4,2) NOT NULL
);

INSERT INTO restaurants
SELECT  i,
        'Restaurant ' || i,
        1 + (i % 6),
        (ARRAY['pizza','sushi','burger','thai','kebab','vegan'])[1 + (i % 6)],
        (ARRAY['standard','plus','premium'])[1 + (i % 3)],
        12.00 + (i % 8)
FROM generate_series(1, 40) AS i;

CREATE TABLE couriers (
    courier_id   INT PRIMARY KEY,
    vehicle_type TEXT NOT NULL,
    city         TEXT NOT NULL,
    hired_at     DATE NOT NULL,
    status       TEXT NOT NULL
);

INSERT INTO couriers
SELECT  i,
        (ARRAY['bike','scooter','car','walk'])[1 + (i % 4)],
        (ARRAY['Warsaw','Warsaw','Warsaw','Krakow','Krakow','Gdansk'])[1 + (i % 6)],
        DATE '2024-01-01' + (i * 7),
        CASE WHEN i % 20 = 0 THEN 'inactive' ELSE 'active' END
FROM generate_series(1, 60) AS i;
