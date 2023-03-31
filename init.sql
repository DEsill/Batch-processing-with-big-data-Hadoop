DROP TABLE IF EXISTS restaurant_detail;

CREATE TABLE restaurant_detail (
    id VARCHAR(55),	 
    restaurant_name	VARCHAR(55),
    category VARCHAR(55),	 
    esimated_cooking_time float,	 
    latitude float,	 
    longitude float	 
);

\COPY restaurant_detail FROM '/var/lib/postgresql/csv/restaurant_detail.csv' WITH (FORMAT CSV, HEADER true);

DROP TABLE IF EXISTS order_detail;

CREATE TABLE order_detail (
    order_created_timestamp timestamp,
    status VARCHAR(55),	 
    price integer,	 
    discount float,	 
    id VARCHAR(100),	 
    driver_id VARCHAR(100),	 
    user_id	VARCHAR(100),		 
    restaurant_id VARCHAR(55)	 
);

\COPY order_detail FROM '/var/lib/postgresql/csv/order_detail.csv' WITH (FORMAT CSV, HEADER true);