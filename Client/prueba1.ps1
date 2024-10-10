CREATE DATABASE TEC
CREATE TABLE Estudiantes (ID INTEGER, Nombre VARCHAR(20), Edad INTEGER)

SET DATABASE TEC
CREATE TABLE Estudiantes (ID INTEGER, Nombre VARCHAR(20), Edad INTEGER)

INSERT INTO Estudiantes (8, Ana, 30)
INSERT INTO Estudiantes (4, Juan, 24)
INSERT INTO Estudiantes (15, Pedro, 21)
INSERT INTO Estudiantes (1, Santiago, 25)
INSERT INTO Estudiantes (35, Rafael, 18)
INSERT INTO Estudiantes (17, Maria, 19)

SELECT * FROM Estudiantes

CREATE INDEX Edad_Index ON Estudiantes(Edad) OF TYPE BST


SELECT * FROM Estudiantes WHERE Edad < 23 ORDER BY ID ASC
SELECT * FROM Estudiantes WHERE Edad > 23 ORDER BY ID DESC
SELECT * FROM Estudiantes ORDER BY Nombre DESC
SELECT * FROM Estudiantes WHERE Nombre LIKE Pe
SELECT ID, Edad FROM Estudiantes WHERE Nombre = SANTIAGO

INSERT INTO Estudiantes (3, David, 25)
INSERT INTO Estudiantes (3, Holaaaaaaaaaaaaaaaaaaa, 25)
INSERT INTO Estudiantes (Ramon, 345, 32)

CREATE INDEX Estudiante_ID ON Estudiantes(ID) OF TYPE BTREE

