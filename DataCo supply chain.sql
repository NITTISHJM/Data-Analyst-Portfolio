-- 0) (Optional) Enable local infile for this session (needs SUPER or appropriate privileges)
SET SESSION sql_mode = 'STRICT_ALL_TABLES,NO_ZERO_DATE,NO_ZERO_IN_DATE';
SET SESSION wait_timeout = 28800;
SET SESSION net_read_timeout = 600;
SET SESSION net_write_timeout = 600;
SET SESSION time_zone = '+00:00';
SET GLOBAL local_infile = 1;

-- 1) Create database
DROP DATABASE IF EXISTS supply_chain_db;
CREATE DATABASE supply_chain_db
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_0900_ai_ci;
USE supply_chain_db;

-- 2) Create a staging table that mirrors CSV headers as-is (all TEXT/VARCHAR to avoid parse errors)
DROP TABLE IF EXISTS dataco_raw;
CREATE TABLE dataco_raw (
   Type                               VARCHAR(50),
   Days_for_shipping_real             INT,
   Days_for_shipment_scheduled        INT,
   Benefit_per_order                  DECIMAL(18,4),
   Sales_per_customer                 DECIMAL(18,4),
   Delivery_Status                    VARCHAR(50),
   Late_delivery_risk                 INT,
   Category_Id                        INT,
   Category_Name                      VARCHAR(255),
   Customer_City                      VARCHAR(255),
   Customer_Country                   VARCHAR(255),
   Customer_Email                     VARCHAR(255),
   Customer_Fname                     VARCHAR(255),
   Customer_Id                        BIGINT,
   Customer_Lname                     VARCHAR(255),
   Customer_Password                  VARCHAR(255),
   Customer_Segment                   VARCHAR(100),
   Customer_State                     VARCHAR(255),
   Customer_Street                    VARCHAR(255),
   Customer_Zipcode                   VARCHAR(50),
   Department_Id                      INT,
   Department_Name                    VARCHAR(255),
   Latitude                           DECIMAL(10,6),
   Longitude                          DECIMAL(10,6),
   Market                             VARCHAR(100),
   Order_City                         VARCHAR(255),
   Order_Country                      VARCHAR(255),
   Order_Customer_Id                  BIGINT,
   order_date                         VARCHAR(50),  -- keep raw for parsing
   Order_Id                           BIGINT,
   Order_Item_Cardprod_Id             BIGINT,
   Order_Item_Discount                DECIMAL(18,4),
   Order_Item_Discount_Rate           DECIMAL(18,6),
   Order_Item_Id                      BIGINT,
   Order_Item_Product_Price           DECIMAL(18,4),
   Order_Item_Profit_Ratio            DECIMAL(18,6),
   Order_Item_Quantity                INT,
   Sales                              DECIMAL(18,4),
   Order_Item_Total                   DECIMAL(18,4),
   Order_Profit_Per_Order             DECIMAL(18,4),
   Order_Region                       VARCHAR(255),
   Order_State                        VARCHAR(255),
   Order_Status                       VARCHAR(100),
   Product_Card_Id                    BIGINT,
   Product_Category_Id                INT,
   Product_Description                TEXT,
   Product_Image                      TEXT,
   Product_Name                       VARCHAR(255),
   Product_Price                      DECIMAL(18,4),
   Product_Status                     INT,
   shipping_date                      VARCHAR(50),  -- keep raw for parsing
   Shipping_Mode                      VARCHAR(100)
)ENGINE=InnoDB;

-- 3) Load CSV into staging (adjust LINES TERMINATED BY if your file is CRLF)
LOAD DATA LOCAL INFILE 'C:/Users/ksa/Documents/Python projects/MYSQL/portfolio/DataCoSupplyChainDataset.csv'
INTO TABLE dataco_raw
CHARACTER SET latin1
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

-- 4) Core analytics schema (normalized)

DROP TABLE IF EXISTS customers;
CREATE TABLE customers (
  customer_id         BIGINT PRIMARY KEY,
  first_name          VARCHAR(255),
  last_name           VARCHAR(255),
  email               VARCHAR(255),
  segment             VARCHAR(100),
  city                VARCHAR(255),
  state               VARCHAR(255),
  country             VARCHAR(255),
  street              VARCHAR(255),
  zipcode             VARCHAR(50)
) ENGINE=InnoDB;

DROP TABLE IF EXISTS products;
CREATE TABLE products (
  product_card_id     BIGINT PRIMARY KEY,
  product_name        VARCHAR(255),
  product_category_id INT,
  category_id         INT,
  category_name       VARCHAR(255),
  product_price       DECIMAL(18,4),
  product_status      INT
) ENGINE=InnoDB;

DROP TABLE IF EXISTS orders;
CREATE TABLE orders (
  order_id            BIGINT PRIMARY KEY,
  customer_id         BIGINT,
  order_status        VARCHAR(100),
  order_region        VARCHAR(255),
  order_state         VARCHAR(255),
  order_country       VARCHAR(255),
  order_city          VARCHAR(255),
  market              VARCHAR(100),
  order_datetime      DATETIME,
  ship_datetime       DATETIME,
  days_ship_scheduled INT,
  days_ship_real      INT,
  late_delivery_risk  TINYINT,
  delivery_status     VARCHAR(50),
  shipping_mode       VARCHAR(100),
  order_profit        DECIMAL(18,4),
  sales_per_customer  DECIMAL(18,4),
  benefit_per_order   DECIMAL(18,4),
  INDEX idx_orders_customer (customer_id),
  INDEX idx_orders_region (order_region),
  INDEX idx_orders_dates (order_datetime, ship_datetime),
  CONSTRAINT fk_orders_customer FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
) ENGINE=InnoDB;

DROP TABLE IF EXISTS order_items;
CREATE TABLE order_items (
  order_item_id           BIGINT PRIMARY KEY,
  order_id                BIGINT,
  product_card_id         BIGINT,
  order_item_quantity     INT,
  order_item_price        DECIMAL(18,4),
  order_item_total        DECIMAL(18,4),
  order_item_discount     DECIMAL(18,4),
  order_item_discount_rate DECIMAL(18,6),
  order_item_profit_ratio DECIMAL(18,6),
  sales                   DECIMAL(18,4),
  INDEX idx_items_order (order_id),
  INDEX idx_items_product (product_card_id),
  CONSTRAINT fk_items_order FOREIGN KEY (order_id) REFERENCES orders(order_id),
  CONSTRAINT fk_items_product FOREIGN KEY (product_card_id) REFERENCES products(product_card_id)
) ENGINE=InnoDB;

-- 5) Populate dimensions

INSERT INTO customers (customer_id, first_name, last_name, email, segment, city, state, country, street, zipcode)
SELECT DISTINCT
  Customer_Id,
  NULLIF(Customer_Fname, ''),
  NULLIF(Customer_Lname, ''),
  NULLIF(Customer_Email, ''),
  NULLIF(Customer_Segment, ''),
  NULLIF(Customer_City, ''),
  NULLIF(Customer_State, ''),
  NULLIF(Customer_Country, ''),
  NULLIF(Customer_Street, ''),
  NULLIF(Customer_Zipcode, '')
FROM dataco_raw
WHERE Customer_Id IS NOT NULL;

INSERT INTO products (
    product_card_id, 
    product_name, 
    product_category_id, 
    category_id, 
    category_name, 
    product_price, 
    product_status
)
SELECT DISTINCT
    CAST(NULLIF(t.Product_Card_Id, '') AS UNSIGNED),
    NULLIF(t.Product_Name, ''),
    CAST(NULLIF(t.Product_Category_Id, '') AS UNSIGNED),
    CAST(NULLIF(t.Category_Id, '') AS UNSIGNED),
    NULLIF(t.Category_Name, ''),
    CAST(NULLIF(t.Product_Price, '') AS DECIMAL(18,4)),
    NULLIF(t.Product_Status, '')
FROM (
    SELECT *
    FROM dataco_raw
    WHERE Product_Card_Id IS NOT NULL
      AND Product_Card_Id NOT IN ('', 'Product_Card_Id')
) AS t
ON DUPLICATE KEY UPDATE
    product_name = VALUES(product_name),
    product_category_id = VALUES(product_category_id),
    category_id = VALUES(category_id),
    category_name = VALUES(category_name),
    product_price = VALUES(product_price),
    product_status = VALUES(product_status);
  

-- 6) Populate orders (parse dates)
-- Dates are like "1/18/2018 12:27" -> use %m/%d/%Y %H:%i. If seconds exist, MySQL will still parse or you can switch to %H:%i:%s.
INSERT INTO orders (
  order_id, customer_id, order_status, order_region, order_state, order_country, order_city, market,
  order_datetime, ship_datetime, days_ship_scheduled, days_ship_real, late_delivery_risk, delivery_status,
  shipping_mode, order_profit, sales_per_customer, benefit_per_order
)
SELECT
  r.Order_Id,
  r.Order_Customer_Id,
  r.Order_Status,
  r.Order_Region,
  r.Order_State,
  r.Order_Country,
  r.Order_City,
  r.Market,
  STR_TO_DATE(r.order_date, '%m/%d/%Y %H:%i'),
  STR_TO_DATE(r.shipping_date, '%m/%d/%Y %H:%i'),
  r.Days_for_shipment_scheduled,
  r.Days_for_shipping_real,
  r.Late_delivery_risk,
  r.Delivery_Status,
  r.Shipping_Mode,
  r.Order_Profit_Per_Order,
  r.Sales_per_customer,
  r.Benefit_per_order
FROM dataco_raw r
WHERE r.Order_Id IS NOT NULL;

-- 7) Populate order_items
INSERT INTO order_items (
  order_item_id, order_id, product_card_id, order_item_quantity, order_item_price,
  order_item_total, order_item_discount, order_item_discount_rate, order_item_profit_ratio, sales
)
SELECT
  r.Order_Item_Id,
  r.Order_Id,
  r.Order_Item_Cardprod_Id,
  r.Order_Item_Quantity,
  r.Order_Item_Product_Price,
  r.Order_Item_Total,
  r.Order_Item_Discount,
  r.Order_Item_Discount_Rate,
  r.Order_Item_Profit_Ratio,
  r.Sales
FROM dataco_raw r
WHERE r.Order_Item_Id IS NOT NULL;

-- 8) Helpful indexes (already added most; add geo for speed if mapping)
CREATE INDEX idx_orders_country_state ON orders(order_country, order_state);
CREATE INDEX idx_customers_country_state ON customers(country, state);

-- 9) Basic sanity checks
SELECT COUNT(*) AS raw_rows FROM dataco_raw;
SELECT COUNT(*) AS customers FROM customers;
SELECT COUNT(*) AS products FROM products;
SELECT COUNT(*) AS orders FROM orders;
SELECT COUNT(*) AS order_items FROM order_items;

-- 10) Quick KPIs moke tests
-- Avg delivery days (real)
SELECT ROUND(AVG(DATEDIFF(ship_datetime, order_datetime)),2) AS avg_days_to_ship
FROM orders
WHERE ship_datetime IS NOT NULL AND order_datetime IS NOT NULL;

-- On-time vs delayed (using scheduled threshold)
SELECT
  CASE WHEN days_ship_real <= days_ship_scheduled THEN 'On-Time' ELSE 'Delayed' END AS status,
  COUNT(*) AS cnt
FROM orders
GROUP BY status;

-- Revenue by region (from item totals)
SELECT o.order_region, ROUND(SUM(oi.order_item_total),2) AS revenue
FROM orders o
JOIN order_items oi USING(order_id)
GROUP BY o.order_region
ORDER BY revenue DESC;
