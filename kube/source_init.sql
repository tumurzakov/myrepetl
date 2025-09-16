-- ETL Tool - MySQL Initialization Script
-- This script sets up the source database with sample data

-- Create replication user
CREATE USER IF NOT EXISTS 'replication_user'@'%' IDENTIFIED BY 'replication_password';
GRANT REPLICATION SLAVE, SUPER, REPLICATION CLIENT ON *.* TO 'replication_user'@'%';
GRANT SELECT ON *.* TO 'replication_user'@'%';
FLUSH PRIVILEGES;

-- Create sample database and tables
CREATE DATABASE IF NOT EXISTS source_db;
USE source_db;

-- Users table
CREATE TABLE IF NOT EXISTS users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    status ENUM('active', 'inactive', 'pending', 'suspended') DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Categories table
CREATE TABLE IF NOT EXISTS categories (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    parent_id INT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (parent_id) REFERENCES categories(id)
);

-- Products table
CREATE TABLE IF NOT EXISTS products (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    price DECIMAL(10,2) NOT NULL,
    category_id INT NOT NULL,
    in_stock BOOLEAN DEFAULT TRUE,
    attributes JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (category_id) REFERENCES categories(id)
);

-- Orders table
CREATE TABLE IF NOT EXISTS orders (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    product_id INT NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status ENUM('pending', 'processing', 'shipped', 'delivered', 'cancelled') DEFAULT 'pending',
    shipping_info JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id),
    FOREIGN KEY (product_id) REFERENCES products(id)
);

-- User events table (for analytics)
CREATE TABLE IF NOT EXISTS user_events (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    event_data JSON,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    session_id VARCHAR(255),
    FOREIGN KEY (user_id) REFERENCES users(id)
);

-- Insert sample data
INSERT INTO categories (name, description, parent_id) VALUES
('Electronics', 'Electronic devices and accessories', NULL),
('Computers', 'Computers and laptops', 1),
('Smartphones', 'Mobile phones and accessories', 1),
('Books', 'Books and literature', NULL),
('Fiction', 'Fiction books', 4),
('Non-Fiction', 'Non-fiction books', 4);

INSERT INTO products (name, description, price, category_id, attributes) VALUES
('MacBook Pro', 'Apple MacBook Pro 13-inch', 1299.99, 2, '{"brand": "Apple", "model": "MacBook Pro", "screen_size": "13-inch"}'),
('iPhone 14', 'Apple iPhone 14', 799.99, 3, '{"brand": "Apple", "model": "iPhone 14", "storage": "128GB"}'),
('Samsung Galaxy S23', 'Samsung Galaxy S23', 699.99, 3, '{"brand": "Samsung", "model": "Galaxy S23", "storage": "256GB"}'),
('Dell XPS 13', 'Dell XPS 13 laptop', 999.99, 2, '{"brand": "Dell", "model": "XPS 13", "screen_size": "13-inch"}'),
('The Great Gatsby', 'Classic American novel', 12.99, 5, '{"author": "F. Scott Fitzgerald", "year": 1925}'),
('Python Programming', 'Learn Python programming', 49.99, 6, '{"author": "Various", "pages": 500}');

INSERT INTO users (name, email, status) VALUES
('John Doe', 'john.doe@example.com', 'active'),
('Jane Smith', 'jane.smith@example.com', 'active'),
('Bob Johnson', 'bob.johnson@example.com', 'inactive'),
('Alice Brown', 'alice.brown@example.com', 'pending'),
('Charlie Wilson', 'charlie.wilson@example.com', 'active');

INSERT INTO orders (user_id, product_id, amount, status, shipping_info) VALUES
(1, 1, 1299.99, 'delivered', '{"address": "123 Main St", "city": "New York", "zip": "10001"}'),
(1, 2, 799.99, 'shipped', '{"address": "123 Main St", "city": "New York", "zip": "10001"}'),
(2, 3, 699.99, 'processing', '{"address": "456 Oak Ave", "city": "Los Angeles", "zip": "90210"}'),
(2, 4, 999.99, 'pending', '{"address": "456 Oak Ave", "city": "Los Angeles", "zip": "90210"}'),
(5, 5, 12.99, 'delivered', '{"address": "789 Pine St", "city": "Chicago", "zip": "60601"}');

INSERT INTO user_events (user_id, event_type, event_data, session_id) VALUES
(1, 'login', '{"ip": "192.168.1.1", "user_agent": "Mozilla/5.0"}', 'sess_001'),
(1, 'page_view', '{"page": "/products", "duration": 30}', 'sess_001'),
(1, 'purchase', '{"product_id": 1, "amount": 1299.99}', 'sess_001'),
(2, 'login', '{"ip": "192.168.1.2", "user_agent": "Mozilla/5.0"}', 'sess_002'),
(2, 'page_view', '{"page": "/products", "duration": 45}', 'sess_002'),
(3, 'login', '{"ip": "192.168.1.3", "user_agent": "Chrome/91.0"}', 'sess_003');

-- Create indexes for better performance
CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_users_status ON users(status);
CREATE INDEX idx_products_category ON products(category_id);
CREATE INDEX idx_products_name ON products(name);
CREATE INDEX idx_orders_user ON orders(user_id);
CREATE INDEX idx_orders_product ON orders(product_id);
CREATE INDEX idx_orders_date ON orders(order_date);
CREATE INDEX idx_user_events_user ON user_events(user_id);
CREATE INDEX idx_user_events_type ON user_events(event_type);
CREATE INDEX idx_user_events_timestamp ON user_events(timestamp);

-- Show tables
SHOW TABLES;

-- Show sample data
SELECT 'Users' as table_name, COUNT(*) as count FROM users
UNION ALL
SELECT 'Categories', COUNT(*) FROM categories
UNION ALL
SELECT 'Products', COUNT(*) FROM products
UNION ALL
SELECT 'Orders', COUNT(*) FROM orders
UNION ALL
SELECT 'User Events', COUNT(*) FROM user_events;
