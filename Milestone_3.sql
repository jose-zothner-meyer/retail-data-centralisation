-- Milestone 3

/*
TASK 1: Cast the columns of the orders_table to the correct data types

Change the data types to correspond to those seen in the table below.
+------------------+--------------------+--------------------+
|   orders_table   | current data type  | required data type |
+------------------+--------------------+--------------------+
| date_uuid        | TEXT               | UUID               |
| user_uuid        | TEXT               | UUID               |
| card_number      | TEXT               | VARCHAR(?)         |
| store_code       | TEXT               | VARCHAR(?)         |
| product_code     | TEXT               | VARCHAR(?)         |
| product_quantity | BIGINT             | SMALLINT           |
+------------------+--------------------+--------------------+
The ? in VARCHAR should be replaced with an integer representing the maximum length of the values in that column.
*/
-- Step 1: Rename the table
ALTER TABLE dim_orders
RENAME TO orders_table

-- Step 2: Check for the data types
SELECT column_name, data_type, character_maximum_length
FROM information_schema.columns
WHERE table_name = 'orders_table';

-- Step 3:
SELECT MAX(LENGTH(card_number::TEXT)) AS max_length_card_number FROM orders_table; -- Replaced with actual max length: 19
SELECT MAX(LENGTH(store_code::TEXT)) AS max_length_store_code FROM orders_table; -- Replaced with actual max length: 12
SELECT MAX(LENGTH(product_code::TEXT)) AS max_length_product_code FROM orders_table; -- Replaced with actual max length: 11

-- Step 4: 
ALTER TABLE orders_table
    ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID,
    ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID,
    ALTER COLUMN card_number TYPE VARCHAR(19),
    ALTER COLUMN store_code TYPE VARCHAR(12),
    ALTER COLUMN product_code TYPE VARCHAR(11),
    ALTER COLUMN product_quantity TYPE SMALLINT;

-- Final Step: Check if the alteration worked as instructed
SELECT column_name, data_type, character_maximum_length
FROM information_schema.columns
WHERE table_name = 'orders_table';

/*
TASK 2: Cast the columns of the dim_users to the correct data types

The column required to be changed in the users table are as follows:
+----------------+--------------------+--------------------+
| dim_users      | current data type  | required data type |
+----------------+--------------------+--------------------+
| first_name     | TEXT               | VARCHAR(255)       |
| last_name      | TEXT               | VARCHAR(255)       |
| date_of_birth  | TEXT               | DATE               |
| country_code   | TEXT               | VARCHAR(?)         |
| user_uuid      | TEXT               | UUID               |
| join_date      | TEXT               | DATE               |
+----------------+--------------------+--------------------+
*/
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'dim_users';

SELECT MAX(LENGTH(country_code)) AS max_length_country_code FROM dim_users;

ALTER TABLE dim_users
    ALTER COLUMN first_name TYPE VARCHAR(255),
    ALTER COLUMN last_name TYPE VARCHAR(255),
    ALTER COLUMN date_of_birth TYPE DATE USING date_of_birth::DATE,
    ALTER COLUMN country_code TYPE VARCHAR(2), -- Replaced with actual max length: 2
    ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID,
    ALTER COLUMN join_date TYPE DATE USING join_date::DATE;

-- Final Step: Check if the alteration worked as instructed
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'dim_users';

/*
TASK 3: Update the dim_store_details

There are two latitude columns in the store details table. Using SQL, merge one of the columns into the other so you have one latitude column.

Then set the data types for each column as shown below:
+---------------------+-------------------+------------------------+
| store_details_table | current data type |   required data type   |
+---------------------+-------------------+------------------------+
| longitude           | TEXT              | NUMERIC                |
| locality            | TEXT              | VARCHAR(255)           |
| store_code          | TEXT              | VARCHAR(?)             |
| staff_numbers       | TEXT              | SMALLINT               |
| opening_date        | TEXT              | DATE                   |
| store_type          | TEXT              | VARCHAR(255) NULLABLE  |
| latitude            | TEXT              | NUMERIC                |
| country_code        | TEXT              | VARCHAR(?)             |
| continent           | TEXT              | VARCHAR(255)           |
+---------------------+-------------------+------------------------+
There is a row that represents the business's website change the location column values from N/A to NULL. 
*/
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'dim_store_details';

SELECT *
FROM dim_store_details;

-- Step 1: Find the character length from the columns
SELECT MAX(LENGTH(store_code)) FROM dim_store_details;
SELECT MAX(LENGTH(country_code)) FROM dim_store_details;

DELETE FROM dim_store_details 
WHERE longitude ~ '[^0-9.-]';

-- Step 2: Update the data types for each column
ALTER TABLE dim_store_details
	ALTER COLUMN longitude TYPE NUMERIC USING longitude::NUMERIC,
	ALTER COLUMN locality TYPE VARCHAR(255),
	ALTER COLUMN store_code TYPE VARCHAR(11),  -- Correct length for store_code
	ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::SMALLINT,
	ALTER COLUMN opening_date TYPE DATE USING opening_date::DATE,
	ALTER COLUMN store_type TYPE VARCHAR(255), -- Nullable, as per requirement
	ALTER COLUMN latitude TYPE NUMERIC USING latitude::NUMERIC,
	ALTER COLUMN country_code TYPE VARCHAR(2), -- Correct length for country_code
	ALTER COLUMN continent TYPE VARCHAR(255);

-- Final Step: Check if the alteration worked as instructed
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'dim_store_details';

/* 
TASK 4: Make changes to the dim_products table for the delivery_team

You will need to do some work on the products table before casting the data types correctly.
The product_price column has a £ character which you need to remove using SQL.
The team that handles the deliveries would like a new human-readable column added for the weight so they can quickly make decisions on delivery weights.

Add a new column weight_class which will contain human-readable values based on the weight range of the product.
+--------------------------+-------------------+
| weight_class VARCHAR(?)  | weight range(kg)  |
+--------------------------+-------------------+
| Light                    | < 2               |
| Mid_Sized                | >= 2 - < 40       |
| Heavy                    | >= 40 - < 140     |
| Truck_Required           | => 140            |
+----------------------------+-----------------+
*/

SELECT *
FROM dim_products;

SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'dim_products';

ALTER TABLE dim_products
ADD COLUMN weight_class VARCHAR(20);

UPDATE dim_products
SET weight_class = CASE
    WHEN weight_kg < 2 THEN 'Light'
    WHEN weight_kg >= 2 AND weight_kg < 40 THEN 'Mid_Sized'
    WHEN weight_kg >= 40 AND weight_kg < 140 THEN 'Heavy'
    WHEN weight_kg >= 140 THEN 'Truck_Required'
    ELSE 'Unknown'
END;

DELETE FROM dim_products
WHERE product_name IS NULL 
AND product_price_gbp IS NULL 
AND category IS NULL 
AND date_added IS NULL;

/*
TASK 5: Update the dim_products table with the required data types

After all the columns are created and cleaned, change the data types of the products table.
You will want to rename the removed column to still_available before changing its data type.

Make the changes to the columns to cast them to the following data types:
+-----------------+--------------------+--------------------+
|  dim_products   | current data type  | required data type |
+-----------------+--------------------+--------------------+
| product_price   | TEXT               | NUMERIC            |
| weight          | TEXT               | NUMERIC            |
| EAN             | TEXT               | VARCHAR(?)         |
| product_code    | TEXT               | VARCHAR(?)         |
| date_added      | TEXT               | DATE               |
| uuid            | TEXT               | UUID               |
| still_available | TEXT               | BOOL               |
| weight_class    | TEXT               | VARCHAR(?)         |
+-----------------+--------------------+--------------------+
*/

SELECT MAX(LENGTH("EAN")) FROM dim_products;
SELECT MAX(LENGTH(product_code)) FROM dim_products;
SELECT MAX(LENGTH(weight_class)) FROM dim_products;

-- Rename the column
ALTER TABLE dim_products
	RENAME COLUMN removed TO still_available;

-- Change the data types
ALTER TABLE dim_products
	ALTER COLUMN product_price_gbp TYPE FLOAT USING product_price_gbp::FLOAT,
	ALTER COLUMN weight_kg TYPE FLOAT USING weight_kg::FLOAT,
	ALTER COLUMN "EAN" TYPE VARCHAR(17) USING "EAN"::VARCHAR(17),
	ALTER COLUMN product_code TYPE VARCHAR(11) USING product_code::VARCHAR(11),
	ALTER COLUMN date_added TYPE DATE USING date_added::DATE,
	ALTER COLUMN "uuid" TYPE UUID USING "uuid"::UUID,
	ALTER COLUMN still_available TYPE BOOLEAN USING CASE WHEN still_available = 'true' THEN true ELSE false END,
	ALTER COLUMN weight_class TYPE VARCHAR(14) USING weight_class::VARCHAR(14);

/*
TASK 6: Update the dim_date_times table

Now update the date table with the correct types:
+-----------------+-------------------+--------------------+
| dim_date_times  | current data type | required data type |
+-----------------+-------------------+--------------------+
| month           | TEXT              | VARCHAR(?)         |
| year            | TEXT              | VARCHAR(?)         |
| day             | TEXT              | VARCHAR(?)         |
| time_period     | TEXT              | VARCHAR(?)         |
| date_uuid       | TEXT              | UUID               |
+-----------------+-------------------+--------------------+
*/
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'dim_date_times';

SELECT *
FROM dim_date_times;

ALTER TABLE dim_date_times
	ADD COLUMN year INT,
	ADD COLUMN month INT,
	ADD COLUMN day INT,
	ADD COLUMN time VARCHAR(8); -- Adjust length for time format (e.g., HH:MM:SS)

UPDATE dim_date_times
SET year = EXTRACT(YEAR FROM datetime),
    month = EXTRACT(MONTH FROM datetime),
    day = EXTRACT(DAY FROM datetime),
    time = TO_CHAR(datetime, 'HH24:MI:SS');

-- Step 1: Change column types to VARCHAR with appropriate lengths
ALTER TABLE dim_date_times
	ALTER COLUMN month TYPE VARCHAR(2), -- Assuming month is stored as a 2-digit value, e.g., "01" for January
	ALTER COLUMN year TYPE VARCHAR(4), -- Assuming year is stored as a 4-digit value, e.g., "2023"
	ALTER COLUMN day TYPE VARCHAR(2), -- Assuming day is stored as a 2-digit value, e.g., "01" for the first day of the month
	ALTER COLUMN time_period TYPE VARCHAR(10); -- Adjust length based on the format of time_period (e.g., "morning")

-- Step 2: Update `date_uuid` column to UUID type
ALTER TABLE dim_date_times
	ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;


SELECT *
FROM dim_date_times;

/*
TASK 7: Updating the dim_card_details table

Now we need to update the last table for the card details.
Make the associated changes after finding out what the lengths of each variable should be:
+------------------------+-------------------+--------------------+
|    dim_card_details    | current data type | required data type |
+------------------------+-------------------+--------------------+
| card_number            | TEXT              | VARCHAR(?)         |
| expiry_date            | TEXT              | VARCHAR(?)         |
| date_payment_confirmed | TEXT              | DATE               |
+------------------------+-------------------+--------------------+
*/

SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'dim_card_details';

-- Step 1: Find the character length from the columns
SELECT MAX(LENGTH(card_number)) FROM dim_card_details;
SELECT MAX(LENGTH(expiry_date)) FROM dim_card_details;

ALTER TABLE dim_card_details
	ALTER COLUMN card_number TYPE VARCHAR(19) USING card_number::VARCHAR(19),
	ALTER COLUMN expiry_date TYPE VARCHAR(5) USING expiry_date::VARCHAR(5),
	ALTER COLUMN date_payment_confirmed TYPE DATE USING date_payment_confirmed::DATE;

/*
TASK 8: Create the primary keys in the dimension tables

Now that the tables have the appropriate data types we can begin adding the primary keys to each of the tables prefixed with dim.

Each table will serve the orders_table which will be the single source of truth for our orders.
Check the column header of the orders_table you will see all but one of the columns exist in one of our tables prefixed with dim.

We need to update the columns in the dim tables with a primary key that matches the same column in the orders_table.
Using SQL, update the respective columns as primary key columns.
*/

-- Step 1: Alter tables to add primary key constraints
SELECT DISTINCT user_uuid
FROM orders_table
WHERE user_uuid NOT IN (
    SELECT user_uuid
    FROM dim_users
);

SELECT store_code
    FROM dim_store_details
WHERE store_code is NULL;

SELECT *
FROM dim_store_details
WHERE store_code is NULL;
-- Add primary key
DELETE FROM dim_users
WHERE user_uuid IS NULL;
ALTER TABLE dim_users
ADD PRIMARY KEY (user_uuid);

-- Add primary key
DELETE FROM dim_store_details
WHERE store_code IS NULL;
ALTER TABLE dim_store_details
ADD PRIMARY KEY (store_code);

-- Add primary key
DELETE FROM dim_products
WHERE product_code IS NULL;
ALTER TABLE dim_products
ADD PRIMARY KEY (product_code);

-- Add primary key
ALTER TABLE dim_date_times
ADD PRIMARY KEY (date_uuid);

-- Add primary key
DELETE FROM dim_card_details
WHERE card_number IS NULL;
ALTER TABLE dim_card_details
ADD PRIMARY KEY (card_number);

-- Confirm the changes with the following queries:
-- 1
SELECT DISTINCT
    tc.table_name AS table_name,
    kcu.column_name AS column_name
FROM information_schema.table_constraints AS tc
JOIN information_schema.key_column_usage AS kcu
ON tc.constraint_name = kcu.constraint_name
WHERE tc.constraint_type = 'PRIMARY KEY'
  AND tc.table_name IN ('dim_users', 'dim_store_details', 'dim_products', 'dim_date_times', 'dim_card_details');
-- 2
SELECT * FROM dim_users LIMIT 10;
SELECT * FROM dim_store_details LIMIT 10;
SELECT * FROM dim_products LIMIT 10;
SELECT * FROM dim_date_times LIMIT 10;
SELECT * FROM dim_card_details LIMIT 10;


/*
TASK 9: Finalising the star-based schema & adding the foreign keys to the orders table

With the primary keys created in the tables prefixed with dim we can now create the foreign keys in the orders_table to reference the primary keys in the other tables.

Use SQL to create those foreign key constraints that reference the primary keys of the other table.
This makes the star-based database schema complete.
*/


-- Add foreign key to reference dim_users
ALTER TABLE orders_table
ADD CONSTRAINT fk_user_uuid
FOREIGN KEY (user_uuid)
REFERENCES dim_users(user_uuid);

-- TO SOLVE FROM HERE

/*
----
step 1

ALTER TABLE dim_users
ADD PRIMARY KEY (user_uuid);

step 2

SELECT DISTINCT store_code
FROM orders_table
WHERE store_code NOT IN (
    SELECT store_code
    FROM dim_store_details
);

step 3 -- Rather than deleting the row, look at the python method.

SELECT *
FROM dim_users
WHERE user_uuid is NULL;
*/


-- Add foreign key to reference dim_store_details --

SELECT DISTINCT store_code
FROM orders_table
WHERE store_code NOT IN (
    SELECT store_code
    FROM dim_store_details
);

/*
DELETE FROM orders_table
WHERE store_code = 'WEB-1388012W';

ALTER TABLE orders_table
ADD CONSTRAINT fk_store_code
FOREIGN KEY (store_code)
REFERENCES dim_store_details(store_code);
*/

-- TO SOLVE UNTIL HERE

-- Add foreign key to reference dim_products
ALTER TABLE orders_table
ADD CONSTRAINT fk_product_code
FOREIGN KEY (product_code)
REFERENCES dim_products(product_code);

-- Add foreign key to reference dim_date_times
ALTER TABLE orders_table
ADD CONSTRAINT fk_date_uuid
FOREIGN KEY (date_uuid)
REFERENCES dim_date_times(date_uuid);

-- Add foreign key to reference dim_card_details
ALTER TABLE orders_table
ADD CONSTRAINT fk_card_number
FOREIGN KEY (card_number)
REFERENCES dim_card_details(card_number);

----

-- Milestone 4

/*
Task 1: How many stores does the business have and in which countries?

The Operations team would like to know which countries we currently operate in and which country now has the most physical stores.
Perform a query on the database to get the information, it should return the following information:
+----------+-----------------+
| country  | total_no_stores |
+----------+-----------------+
| GB       |             265 |
| DE       |             141 |
| US       |              34 |
+----------+-----------------+
Note: DE is short for Deutschland(Germany)
*/

SELECT 
    country_code AS country,
    COUNT(*) AS total_no_stores
FROM 
    dim_store_details
GROUP BY 
    country_code
ORDER BY 
    total_no_stores DESC;

/*
Task 2: Which location currently have the most stores?

The business stakeholders would like to know which locations currently have the most stores.
They would like to close some stores before opening more in other locations.
Find out which locations have the most stores currently. The query should return the following:
+-------------------+-----------------+
|     locality      | total_no_stores |
+-------------------+-----------------+
| Chapletown        |              14 |
| Belper            |              13 |
| Bushley           |              12 |
| Exeter            |              11 |
| High Wycombe      |              10 |
| Arbroath          |              10 |
| Rutherglen        |              10 |
+-------------------+-----------------+
*/

SELECT 
    locality,
    COUNT(*) AS total_no_stores
FROM 
    dim_store_details
GROUP BY 
    locality
ORDER BY 
    total_no_stores DESC
LIMIT 7; -- Limit to the top 7 localities with the most stores

/*
Task 3: Which months produced the largest amount of sales?

Query the database to find out which months have produced the most sales. The query should return the following information:
+-------------+-------+
| total_sales | month |
+-------------+-------+
|   673295.68 |     8 |
|   668041.45 |     1 |
|   657335.84 |    10 |
|   650321.43 |     5 |
|   645741.70 |     7 |
|   645463.00 |     3 |
+-------------+-------+
*/

SELECT
ROUND(CAST(SUM(ord.product_quantity * dp.product_price_gbp) AS NUMERIC), 2)
AS sales, ddt.month
FROM
orders_table ord
INNER JOIN
dim_date_times ddt
ON
ddt.date_uuid = ord.date_uuid
INNER JOIN
dim_products dp
ON
dp.product_code = ord.product_code
GROUP BY
(ddt.month)
ORDER BY sales DESC
LIMIT 6;

/*
Task 4: How many sales are coming from online?

The company is looking to increase its online sales.
They want to know how many sales are happening online vs offline.
Calculate how many products were sold and the amount of sales made for online and offline purchases.
You should get the following information:
+------------------+-------------------------+----------+
| numbers_of_sales | product_quantity_count  | location |
+------------------+-------------------------+----------+
|            26957 |                  107739 | Web      |
|            93166 |                  374047 | Offline  |
+------------------+-------------------------+----------+
*/

SELECT 
    COUNT(store_code) AS numbers_of_sales,
    SUM(product_quantity) AS product_quantity_count,
    'Web' AS location
FROM 
    orders_table
WHERE 
    store_code LIKE 'WEB%'
GROUP BY 
    location

UNION

SELECT 
    COUNT(store_code) AS numbers_of_sales,
    SUM(product_quantity) AS product_quantity_count,
    'Offline' AS location
FROM 
    orders_table
WHERE 
    store_code NOT LIKE 'WEB%'
GROUP BY 
    location;

/*
Task 5: What percentage of sales come through each type of store?

The sales team wants to know which of the different store types is generated the most revenue so they know where to focus.
Find out the total revenue coming from each of the different store types and the number of sales made as a percentage.
The query should return:
+-------------+-------------+-----------------+
| store_type  | total_sales |   sales_made(%) |
+-------------+-------------+-----------------+
| Local       |  3440896.52 |           44.87 |
| Web portal  |  1726547.05 |           22.44 |
| Super Store |  1224293.65 |           15.63 |
| Mall Kiosk  |   698791.61 |            8.96 |
| Outlet      |   631804.81 |            8.10 |
+-------------+-------------+-----------------+
*/
SELECT 
    dim_store_details.store_type,
    ROUND(SUM(orders_table.product_quantity * dim_products.product_price_gbp)::numeric, 2) AS total_sales,
    ROUND(
        (SUM(orders_table.product_quantity * dim_products.product_price_gbp)::numeric / 
        (SELECT SUM(orders_table.product_quantity * dim_products.product_price_gbp)::numeric 
         FROM orders_table 
         JOIN dim_products ON orders_table.product_code = dim_products.product_code) * 100), 2) AS percentage_total
FROM 
    orders_table
JOIN 
    dim_store_details ON orders_table.store_code = dim_store_details.store_code
JOIN 
    dim_products ON orders_table.product_code = dim_products.product_code
GROUP BY 
    dim_store_details.store_type
ORDER BY 
    total_sales DESC;


/*
Task 6: Which month in each year produced the highest cost of sales?

The company stakeholders want assurances that the company has been doing well recently.
Find which months in which years have had the most sales historically.
The query should return the following information:
+-------------+------+-------+
| total_sales | year | month |
+-------------+------+-------+
|    27936.77 | 1994 |     3 |
|    27356.14 | 2019 |     1 |
|    27091.67 | 2009 |     8 |
|    26679.98 | 1997 |    11 |
|    26310.97 | 2018 |    12 |
|    26277.72 | 2019 |     8 |
|    26236.67 | 2017 |     9 |
|    25798.12 | 2010 |     5 |
|    25648.29 | 1996 |     8 |
|    25614.54 | 2000 |     1 |
+-------------+------+-------+
*/


/*
Task 7: What is our staff headcount?

The operations team would like to know the overall staff numbers in each location around the world. Perform a query to determine the staff numbers in each of the countries the company sells in.
The query should return the values:
+---------------------+--------------+
| total_staff_numbers | country_code |
+---------------------+--------------+
|               13307 | GB           |
|                6123 | DE           |
|                1384 | US           |
+---------------------+--------------+
*/


/*
Task 8: Which German store type is selling the most?

The sales team is looking to expand their territory in Germany. Determine which type of store is generating the most sales in Germany.
The query will return:
+--------------+-------------+--------------+
| total_sales  | store_type  | country_code |
+--------------+-------------+--------------+
|   198373.57  | Outlet      | DE           |
|   247634.20  | Mall Kiosk  | DE           |
|   384625.03  | Super Store | DE           |
|  1109909.59  | Local       | DE           |
+--------------+-------------+--------------+
*/


/*
Task 9: How quickly is the company making sales?

Sales would like the get an accurate metric for how quickly the company is making sales.
Determine the average time taken between each sale grouped by year, the query should return the following information:
 +------+-------------------------------------------------------+
 | year |                           actual_time_taken           |
 +------+-------------------------------------------------------+
 | 2013 | "hours": 2, "minutes": 17, "seconds": 12, "millise... |
 | 1993 | "hours": 2, "minutes": 15, "seconds": 35, "millise... |
 | 2002 | "hours": 2, "minutes": 13, "seconds": 50, "millise... | 
 | 2022 | "hours": 2, "minutes": 13, "seconds": 6,  "millise... |
 | 2008 | "hours": 2, "minutes": 13, "seconds": 2,  "millise... |
 +------+-------------------------------------------------------+
 
NOTE: Your result may be displayed differently if using PgAdmin4.

Hint: You will need the SQL LEAD function and create a full timestamp from the date/time values provided.
*/