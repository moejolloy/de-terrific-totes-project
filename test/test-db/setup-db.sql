DROP DATABASE IF EXISTS test_raw_data;

CREATE DATABASE test_raw_data;

\c test_raw_data

CREATE TABLE test_table (
    column_id SERIAL PRIMARY KEY,
    column_2 VARCHAR NOT NULL,
    column_3 INT NOT NULL
);

INSERT INTO test_table (
    column_2,
    column_3
)
VALUES
('row 1', 1),
('row 2', 2);