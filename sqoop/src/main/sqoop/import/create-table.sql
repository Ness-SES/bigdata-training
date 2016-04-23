USE test;

DROP TABLE IF EXISTS import_table;

CREATE TABLE import_table (
  id   INT,
  name VARCHAR(20)
);

INSERT INTO test.import_table
VALUES (0, 'zero'), (1, 'ONE'), (2, 'two'), (3, 'three'), (4, 'four'), (5, 'five'), (6, 'six'), (7, 'seven');
