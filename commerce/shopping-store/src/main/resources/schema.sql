CREATE TABLE IF NOT EXISTS products (
    product_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    product_name VARCHAR(255) NOT NULL,
    description TEXT NOT NULL,
    image_src VARCHAR(500),
    quantity_state VARCHAR(20) NOT NULL CHECK (quantity_state IN ('ENDED', 'FEW', 'ENOUGH', 'MANY')),
    product_state VARCHAR(20) NOT NULL CHECK (product_state IN ('ACTIVE', 'DEACTIVATE')),
    product_category VARCHAR(20) NOT NULL CHECK (product_category IN ('LIGHTING', 'CONTROL', 'SENSORS')),
    price NUMERIC(10,2) NOT NULL CHECK (price >= 0),
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_products_category_state ON products(product_category, product_state);
CREATE INDEX IF NOT EXISTS idx_products_state ON products(product_state);
CREATE INDEX IF NOT EXISTS idx_products_created_at ON products(created_at);

COMMENT ON TABLE products IS 'Таблица для хранения информации о товарах магазина';
COMMENT ON COLUMN products.product_id IS 'Уникальный идентификатор товара';
COMMENT ON COLUMN products.product_name IS 'Наименование товара';
COMMENT ON COLUMN products.description IS 'Описание товара';
COMMENT ON COLUMN products.image_src IS 'Ссылка на изображение товара';
COMMENT ON COLUMN products.quantity_state IS 'Статус количества товара: ENDED, FEW, ENOUGH, MANY';
COMMENT ON COLUMN products.product_state IS 'Статус товара: ACTIVE, DEACTIVATE';
COMMENT ON COLUMN products.product_category IS 'Категория товара: LIGHTING, CONTROL, SENSORS';
COMMENT ON COLUMN products.price IS 'Цена товара';
COMMENT ON COLUMN products.created_at IS 'Дата и время создания записи';
COMMENT ON COLUMN products.updated_at IS 'Дата и время последнего обновления записи';