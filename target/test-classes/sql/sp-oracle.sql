CREATE TABLE "book" (
	"title" varchar2(50),
	"author" varchar2(20),
	"quantity" int,
	"price" float
);

INSERT INTO "book" VALUES ('Peppa Pig: Fun at the Fair', 'Collectif', 23, 3.74);
INSERT INTO "book" VALUES ('Panda Goes to the Olympics', 'Judith Simanovsky', 7, 4.99);
INSERT INTO "book" VALUES ('Topsy and Tim Visit London', 'Jean Adamson', 15, 3.29);

CREATE OR REPLACE PROCEDURE sp_ListBooks (
	title_search IN varchar2,
	total_cost OUT float,
	books_cursor OUT SYS_REFCURSOR
)
AS
BEGIN
	OPEN books_cursor FOR SELECT "title", "author", "quantity", "price" FROM "book" WHERE "title" LIKE title_search;

	SELECT SUM("quantity" * "price") INTO total_cost FROM "book" WHERE "title" LIKE title_search;
END;
