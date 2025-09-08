-- Create Tables
-- PRODUCTS
CREATE TABLE products (
  product_id SERIAL PRIMARY KEY,
  product_name VARCHAR(100),
  category VARCHAR(50),
  unit_price DECIMAL(10,2)
);

-- CUSTOMERS
CREATE TABLE customers (
  customer_id SERIAL PRIMARY KEY,
  customer_name VARCHAR(100),
  email VARCHAR(120),
  country VARCHAR(60)
);

-- ORDERS
CREATE TABLE orders (
  order_id SERIAL PRIMARY KEY,
  customer_id INT REFERENCES customers(customer_id),
  order_date DATE,
  status VARCHAR(20)
);

-- ORDER ITEMS
CREATE TABLE order_items (
  order_item_id SERIAL PRIMARY KEY,
  order_id INT REFERENCES orders(order_id),
  product_id INT REFERENCES products(product_id),
  quantity INT,
  discount DECIMAL(5,2)  -- percent, e.g., 10.00 = 10%
);

-- Seed data (minimal but varied)
INSERT INTO products (product_name, category, unit_price) VALUES
('Laptop 14"', 'Electronics', 1200),
('Headphones', 'Electronics', 200),
('T-Shirt', 'Clothing', 25),
('Shoes', 'Clothing', 60),
('Coffee Maker', 'Home', 90);

INSERT INTO customers (customer_name, email, country) VALUES
('Alice Johnson', 'alice@example.com', 'USA'),
('Bob Smith', 'bob@example.com', 'UK'),
('Chen Wei', NULL, 'China'),
('Diana Prince', 'diana@wonder.com', NULL);

INSERT INTO orders (customer_id, order_date, status) VALUES
(1, '2025-01-15', 'Completed'),
(2, '2025-02-02', 'Completed'),
(3, '2025-02-15', 'Pending'),
(4, '2025-02-18', 'Cancelled'),
(1, '2025-03-01', 'Completed');

INSERT INTO order_items (order_id, product_id, quantity, discount) VALUES
(1, 1, 2, 0.00),     -- 2 Laptops
(1, 2, 1, 5.00),     -- 1 Headphones 5% off
(2, 2, 5, 0.00),     -- 5 Headphones
(3, 3, 3, 0.00),     -- 3 T-Shirts
(4, 5, 1, 10.00),    -- Coffee Maker 10% off
(5, 4, 2, 0.00);     -- 2 Shoes

-- Average Order value (Completed Order only
select o.order_id,p.product_name,o.status,
round(sum(oi.quantity * p.unit_price * (1-oi.discount/100))/count(distinct o.order_id),2)
as avg_product_value
from orders o
join order_items oi on oi.order_id = o.order_id
join products p on p.product_id = oi.product_id
where status = 'Completed'
group by o.order_id,p.product_name,o.status;

-- Category share of revenue in 2025
with category_revenue as (
select p.category,
	sum(oi.quantity * p.unit_price * (1-oi.discount/100)) as revenue
    from order_items oi
    join products p on oi.product_id = p.product_id
    join orders o on oi.order_id = o.order_id
    where extract(year from o.order_date)=2025
    group by p.category
)

select category,
round(revenue/sum(revenue)over()*100,2) as category_share_of_revenue
from category_revenue
order by category_share_of_revenue desc;

-- Customer order count
select c.customer_id,c.customer_name,
count(o.order_id)as total_orders
from customers c
left join orders o on o.customer_id = c.customer_id
group by c.customer_id,c.customer_name;

-- Customer with missing email
select customer_id,customer_name
from customers
where email is null or trim(email)='';

-- flag  highvalueorder if total greater than1000
select o.order_id,
round(sum(oi.quantity * p.unit_price * (1-oi.discount/100)),2) as order_total,
case
	when sum(oi.quantity * p.unit_price *(1-oi.discount/100))>1000 then 'High Value Order'
    else 'Normal Order'
end as order_flag
from orders o
join order_items oi on oi.order_id = o.order_id
join products p on oi.product_id = p.product_id
group by o.order_id;

-- monthly revenue for 2025
WITH months AS (
    SELECT 1 AS month_num, 'January' AS month_name UNION ALL
    SELECT 2, 'February' UNION ALL
    SELECT 3, 'March' UNION ALL
    SELECT 4, 'April' UNION ALL
    SELECT 5, 'May' UNION ALL
    SELECT 6, 'June' UNION ALL
    SELECT 7, 'July' UNION ALL
    SELECT 8, 'August' UNION ALL
    SELECT 9, 'September' UNION ALL
    SELECT 10, 'October' UNION ALL
    SELECT 11, 'November' UNION ALL
    SELECT 12, 'December'
)

-- Step 2: Join with your sales table
SELECT 
    m.month_name,
    COALESCE(SUM(oi.quantity * p.unit_price * (1-discount/100)), 0) AS total_revenue
FROM months m
LEFT JOIN orders o
    ON MONTH(o.order_date) = m.month_num
    AND YEAR(o.order_date) = 2025
left join order_items oi on oi.order_id = o.order_id
join products p on oi.product_id = p.product_id
GROUP BY m.month_num, m.month_name
ORDER BY m.month_num;

-- rank product by revenue within each category
select p.category,p.product_name,
round(sum(oi.quantity * p.unit_price * (1-oi.discount/100)),2) as total_revenue,
rank() over(partition by p.category order by sum(oi.quantity * p.unit_price * (1-oi.discount/100))desc) as revenue_rank
from order_items oi
join products p on oi.product_id = p.product_id
group by p.category,p.product_name;

-- repeat customer greater than or equal to 2
select
	c.customer_id,
    c.customer_name,
    count(distinct o.order_id) as order_count
    from customers c
    join orders o on c.customer_id=o.customer_id
    group by c.customer_id,c.customer_name
    having count(distinct o.order_id)>=2;

-- top 3 product by revenue
select p.product_name,
round(sum(oi.quantity * p.unit_price * (1-oi.discount/100))) as total_revenue
from order_items oi
join products p on oi.product_id = p.product_id
group by p.product_name
order by total_revenue desc
limit 3;

-- total revenue per category
SELECT p.category,
sum(oi.quantity * p.unit_price * 1-oi.discount/100) as 'Total Revenue'
from order_items oi
join products p on oi.product_id = p.product_id
group by p.category
order by 'Total Revenue';
