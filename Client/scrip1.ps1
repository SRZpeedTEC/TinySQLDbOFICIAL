CREATE DATABASE Universidad
SET DATABASE Universidad
CREATE TABLE Estudiantes (ID INTEGER, Nombre VARCHAR(50), Edad INTEGER)
INSERT INTO Estudiantes (1, 'Ana', 20)
INSERT INTO Estudiantes (2, 'Juan', 21)
INSERT INTO Estudiantes (3, 'Pedro', 22)
SELECT * FROM Estudiantes