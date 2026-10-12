CREATE VIEW gold.calendar
AS
SELECT
    *
FROM
    OPENROWSET(
                BULK 'https://projectstorage8896.blob.core.windows.net/silver/AdventureWorks_Calendar/',
                FORMAT = 'PARQUET'
    ) as QUER1

--CREATE VIEW CUSTOMERS 
CREATE VIEW gold.customers
AS
SELECT
    *
FROM
    OPENROWSET(
                BULK 'https://projectstorage8896.blob.core.windows.net/silver/AdventureWorks_Customers/',
                FORMAT = 'PARQUET'
    ) as QUER1


--CREATE PRODUCT SUBCATEGORIES
CREATE VIEW gold.subcat
AS
SELECT
    *
FROM
    OPENROWSET(
                BULK 'https://projectstorage8896.blob.core.windows.net/silver/AdventureWorks_Product_Subcategories/',
                FORMAT = 'PARQUET'
    ) as QUER1

--CREATE  PRODUCTS

CREATE VIEW gold.products
AS
SELECT
    *
FROM
    OPENROWSET(
                BULK 'https://projectstorage8896.blob.core.windows.net/silver/AdventureWorks_Products/',
                FORMAT = 'PARQUET'
    ) as QUER1

--CREATE SALES
CREATE VIEW gold.sales
AS
SELECT
    *
FROM
    OPENROWSET(
                BULK 'https://projectstorage8896.blob.core.windows.net/silver/AdventureWorks_Sales/',
                FORMAT = 'PARQUET'
    ) as QUER1

--CREATE Territories
CREATE VIEW gold.territories
AS
SELECT
    *
FROM
    OPENROWSET(
                BULK 'https://projectstorage8896.blob.core.windows.net/silver/AdventureWorks_Territories/',
                FORMAT = 'PARQUET'
    ) as QUER1
    
-- CREATE Returns
CREATE VIEW gold.returns
AS
SELECT
    *
FROM
    OPENROWSET(
                BULK 'https://projectstorage8896.blob.core.windows.net/silver/Returns/',
                FORMAT = 'PARQUET'
    ) as QUER1    


-- CREATE PRODUCT CATEGORIES
-- (run this after the AdventureWorks_Product_Categories folder exists in the silver container)
CREATE VIEW gold.procat
AS
SELECT
    *
FROM
    OPENROWSET(
                BULK 'https://projectstorage8896.blob.core.windows.net/silver/AdventureWorks_Product_Categories/',
                FORMAT = 'PARQUET'
    ) as QUER1
