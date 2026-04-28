CREATE TABLE IF NOT EXISTS orders (
  order_id     SERIAL PRIMARY KEY,
  user_id      INT         NOT NULL,
  amount       NUMERIC(10, 2) NOT NULL,
status       VARCHAR(32) NOT NULL,
created_at   TIMESTAMP   NOT NULL DEFAULT now()
);

INSERT INTO orders (user_id, amount, status) VALUES
 (1, 120.50, 'PAID'),
 (2,  15.00, 'PAID'),
 (3, 999.99, 'PAID'),
 (4,  42.00, 'CANCELLED'),
 (5, 300.00, 'PAID'),
 (6,   7.50, 'PAID'),
 (7, 180.00, 'REFUNDED'),
 (8,  55.25, 'PAID'),
 (9, 410.10, 'PAID'),
 (10, 25.00, 'PAID');