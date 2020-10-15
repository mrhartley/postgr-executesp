CREATE TABLE book (
	title varchar(50),
	author varchar(20),
	quantity int,
	price float
);

INSERT INTO book VALUES ('Peppa Pig: Fun at the Fair', 'Collectif', 23, 3.74);
INSERT INTO book VALUES ('Panda Goes to the Olympics', 'Judith Simanovsky', 7, 4.99);
INSERT INTO book VALUES ('Topsy and Tim Visit London', 'Jean Adamson', 15, 3.29);

CREATE PROCEDURE sp_ListBooks (
	@title_search varchar(50),
	@total_cost float OUTPUT
)
AS
BEGIN
	SELECT title, author, quantity, price FROM book WHERE title LIKE @title_search;

	SET @total_cost = (SELECT SUM(quantity * price) FROM book WHERE title LIKE @title_search);
END;
