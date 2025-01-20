-- Create main schema
CREATE SCHEMA IF NOT EXISTS main;

-- Create staging schema 
CREATE SCHEMA IF NOT EXISTS staging;

-- Add comment to schemas
COMMENT ON SCHEMA main IS 'Production schema for final tables';
COMMENT ON SCHEMA staging IS 'Staging schema for temporary and intermediate tables';

-- Create a new admin user
CREATE USER airflow WITH PASSWORD 'airflow_password';

-- Grant all privileges on the database to the admin user
GRANT ALL PRIVILEGES ON DATABASE knigomag TO airflow;

-- Grant usage on schemas to airflow user
GRANT USAGE ON SCHEMA staging TO airflow;
GRANT USAGE ON SCHEMA main TO airflow;

-- Grant INSERT, UPDATE, SELECT privileges on store_data table to airflow
GRANT INSERT, UPDATE, SELECT ON TABLE staging.store_data TO airflow;

-- Create Product Dimension Table
CREATE TABLE main.Product_Dimension (
    position_id INT PRIMARY KEY,
    position_name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    subcategory VARCHAR(100),
    supplier VARCHAR(100),
    price DECIMAL(10,2)
    -- Additional attributes as needed
);

-- Create Seller_Dimension Table
CREATE TABLE main.Seller_Dimension (
    seller_id INT PRIMARY KEY,
    seller_name VARCHAR(100) NOT NULL,
    hire_date DATE
    -- Additional attributes as needed
);

-- Create Store_Dimension Table
CREATE TABLE main.Store_Dimension (
    store_id INT PRIMARY KEY,
    store_name VARCHAR(100) NOT NULL,
    location VARCHAR(100)
    -- Additional attributes as needed
);

-- Inventory Fact Table
CREATE TABLE main.Inventory_Fact (
    inventory_id INT PRIMARY KEY,
    inventory_date DATE NOT NULL,  
    store_id INT NOT NULL,
    position_id INT NOT NULL,
    amount INT,
    FOREIGN KEY (store_id) REFERENCES main.Store_Dimension(store_id),
    FOREIGN KEY (position_id) REFERENCES main.Product_Dimension(position_id)
);

-- Sales Fact Table
CREATE TABLE main.Sales_Fact (
    sale_id SERIAL PRIMARY KEY,
    sale_date DATE NOT NULL,
    store_id INT NOT NULL,
    seller_id INT NOT NULL,
    position_id INT NOT NULL,
    revenue DECIMAL(15,2),
    quantity INT,
    price DECIMAL(10,2),

    FOREIGN KEY (store_id) REFERENCES main.Store_Dimension(store_id),
    FOREIGN KEY (seller_id) REFERENCES main.Seller_Dimension(seller_id),
    FOREIGN KEY (position_id) REFERENCES main.Product_Dimension(position_id)
);

-- Office Work Schedule Fact Table
CREATE TABLE main.Schedule_Fact (
    schedule_id INT PRIMARY KEY,
    work_date DATE NOT NULL, 
    store_id INT NOT NULL,
    seller_id INT NOT NULL,
    wage DECIMAL(10,2),
    FOREIGN KEY (store_id) REFERENCES main.Store_Dimension(store_id),
    FOREIGN KEY (seller_id) REFERENCES main.Seller_Dimension(seller_id)
);

-- Description: Create the store_data table
CREATE TABLE staging.store_data (
    date DATE NOT NULL,
    revenue INTEGER NOT NULL,
    balance INTEGER NOT NULL,
    seller_name VARCHAR(100) NOT NULL,
    store_id INTEGER NOT NULL
);

-- Add unique constraint to staging.store_data
ALTER TABLE staging.store_data
ADD CONSTRAINT unique_store_date UNIQUE (store_id, date);
