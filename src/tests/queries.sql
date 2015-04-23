-------------------------------------------------------------------------------
-- Database Setup
-------------------------------------------------------------------------------
DROP TABLE Students;
DROP INDEX IX_Age;

CREATE TABLE Students (sid INTEGER, name STRING(50), age FLOAT);

CREATE INDEX IX_Age ON Students(Age);

INSERT INTO Students VALUES (1, 'Alice', 25.67);
INSERT INTO Students VALUES (2, 'Chris', 12.34);
INSERT INTO Students VALUES (3, 'Bob', 30.0);
INSERT INTO Students VALUES (4, 'Andy', 50.0);
INSERT INTO Students VALUES (5, 'Ron', 30.0);

DELETE Students WHERE name = 'Chris';

STATS

DROP INDEX IX_Age;
DROP TABLE Students;


QUIT
