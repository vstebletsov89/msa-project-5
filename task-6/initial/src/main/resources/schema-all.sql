DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS loyality_data;

CREATE TABLE products (
    productId     BIGINT       NOT NULL PRIMARY KEY,
    productSku    BIGINT       NOT NULL UNIQUE,
    productName   VARCHAR(20),
    productAmount BIGINT,
    productData   VARCHAR(120));

CREATE TABLE loyality_data (
    productSku    BIGINT       NOT NULL PRIMARY KEY,
    loyalityData  VARCHAR(120));

INSERT INTO loyality_data (productSku, loyalityData) VALUES
    (20001, 'Loyality_on'),
    (30001, 'Loyality_on'),
    (50001, 'Loyality_on'),
    (60001, 'Loyality_on')
    ON CONFLICT (productSku) DO NOTHING;