-------------------------------------------------------------------------------
-- Database Setup
-------------------------------------------------------------------------------
-- Database Setup

DROP INDEX IX_Age;
DROP TABLE Students;
DROP TABLE Courses;
DROP TABLE Grades;
DROP TABLE Foo;

CREATE TABLE Students (sid INTEGER, name STRING(50), age FLOAT);
CREATE TABLE Courses (cid INTEGER, title STRING(50));
CREATE TABLE Grades (gsid INTEGER, gcid INTEGER, points FLOAT);
CREATE TABLE Foo (a INTEGER, b INTEGER, c INTEGER, d INTEGER, e INTEGER);

CREATE INDEX IX_Age ON Students(Age);

INSERT INTO Students VALUES (1, 'Alice', 25.67);
INSERT INTO Students VALUES (2, 'Chris', 12.34);
INSERT INTO Students VALUES (3, 'Bob', 30.0);
INSERT INTO Students VALUES (4, 'Andy', 50.0);
INSERT INTO Students VALUES (5, 'Ron', 30.0);

CREATE INDEX IX_Name ON Students(Name);

INSERT INTO Courses VALUES (448, 'DB Fun');
INSERT INTO Courses VALUES (348, 'Less Cool');
INSERT INTO Courses VALUES (542, 'More Fun');

INSERT INTO Grades VALUES (2, 448, 4.0);
INSERT INTO Grades VALUES (3, 348, 2.5);
INSERT INTO Grades VALUES (1, 348, 3.1);
INSERT INTO Grades VALUES (4, 542, 2.8);
INSERT INTO Grades VALUES (5, 542, 3.0);

INSERT INTO Foo VALUES (1, 2, 8, 4, 5);
INSERT INTO Foo VALUES (2, 2, 8, 4, 5);
INSERT INTO Foo VALUES (1, 5, 3, 4, 5);
INSERT INTO Foo VALUES (1, 4, 8, 5, 5);
INSERT INTO Foo VALUES (1, 4, 3, 4, 6);

-------------------------------------------------------------------------------
-- Sample Queries

SELECT sid, name, points FROM Students, Grades WHERE sid = gsid AND points >= 3.0;
SELECT sid, name, points FROM Students, Grades WHERE sid = gsid AND points >= 3.0 OR sid = gsid AND points <= 2.5;
SELECT * FROM Foo WHERE a = 1 and b = 2 or c = 3 and d = 4 and e = 5;
SELECT * FROM Students, Grades WHERE sid = gsid AND age = 30.0;

STATS

DROP INDEX IX_Age;
DROP TABLE Students;
DROP TABLE Courses;
DROP TABLE Grades;
DROP TABLE Foo;

QUIT
