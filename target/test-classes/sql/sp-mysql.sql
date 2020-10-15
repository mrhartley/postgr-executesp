CREATE TABLE book (
	title varchar(50),
	author varchar(20),
	quantity int,
	price float
);

INSERT INTO book VALUES ('Peppa Pig: Fun at the Fair', 'Collectif', 23, 3.74);
INSERT INTO book VALUES ('Panda Goes to the Olympics', 'Judith Simanovsky', 7, 4.99);
INSERT INTO book VALUES ('Topsy and Tim Visit London', 'Jean Adamson', 15, 3.29);

DELIMITER $$
	CREATE PROCEDURE sp_ListBooks (
		IN title_search varchar(50),
		OUT total_cost float
	)
	BEGIN
		SELECT title, author, quantity, price FROM book WHERE title LIKE title_search;

		SELECT SUM(quantity * price) INTO total_cost FROM book WHERE title LIKE title_search;
	END$$
DELIMITER;

DELIMITER $$
	CREATE PROCEDURE ALL_TYPES (
		INOUT aaa SMALLINT,
		INOUT bbb INTEGER,
		INOUT ccc BIGINT,
		INOUT ddd DECIMAL(2, 1),
		INOUT eee REAL,
		INOUT fff DOUBLE,
		INOUT ggg FLOAT,
		INOUT hhh CHAR(5),
		INOUT iii VARCHAR(10),
		INOUT jjj LONG VARCHAR,
		INOUT kkk TEXT,
		INOUT lll DATE,
		INOUT mmm TIME,
		INOUT nnn TIMESTAMP
	)
	BEGIN
		SELECT 1;
	END$$
DELIMITER; 
