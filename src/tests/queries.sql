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

UPDATE Students SET sid = 5 WHERE name = 'Chris';

DELETE Students WHERE name = 'Chris';

-- Test invalid stuff
CREATE INDEX Courses ON Grades(points);
CREATE INDEX IX_Age ON Grades(points);
CREATE INDEX Bad ON Unknown(secret);
CREATE INDEX Bad ON Grades(secret);

DROP INDEX Bad;
DROP TABLE Bad;

INSERT INTO Bad VALUES (1);
INSERT INTO Courses VALUES (1, 'two', 3);
INSERT INTO Courses VALUES ('one', 2);

DELETE Bad;
DELETE Grades WHERE bad = 5;
DELETE Grades WHERE gsid = 'bad';

UPDATE Bad SET Id = 1;
UPDATE Courses SET bad = 1;
UPDATE Courses SET cid = 'bad';


STATS

DROP INDEX IX_Age;
DROP TABLE Students;

QUIT
