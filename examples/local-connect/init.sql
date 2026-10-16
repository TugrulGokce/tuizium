CREATE TABLE customers (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  email TEXT
);
INSERT INTO customers (name, email) VALUES ('Ada Lovelace', 'ada@example.com'), ('Alan Turing', 'alan@example.com');

-- target table for the sink connector
CREATE TABLE customers_copy (
  id INT PRIMARY KEY,
  name TEXT,
  email TEXT
);
