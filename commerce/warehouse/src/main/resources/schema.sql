CREATE TABLE IF NOT EXISTS warehouse_products (
    warehouse_product_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    product_id UUID NOT NULL UNIQUE,
    quantity BIGINT NOT NULL CHECK (quantity >= 0),
    fragile BOOLEAN,
    width DOUBLE PRECISION,
    height DOUBLE PRECISION,
    depth DOUBLE PRECISION,
    weight DOUBLE PRECISION,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    version BIGINT DEFAULT 0
);


CREATE UNIQUE INDEX IF NOT EXISTS idx_warehouse_products_product_id
ON warehouse_products(product_id);

CREATE INDEX IF NOT EXISTS idx_warehouse_products_quantity
ON warehouse_products(quantity);


COMMENT ON TABLE warehouse_products IS 'Таблица для хранения информации о товарах на складе';
COMMENT ON COLUMN warehouse_products.warehouse_product_id IS 'Уникальный идентификатор записи на складе';
COMMENT ON COLUMN warehouse_products.product_id IS 'Идентификатор товара (ссылка на products)';
COMMENT ON COLUMN warehouse_products.quantity IS 'Количество товара на складе';
COMMENT ON COLUMN warehouse_products.fragile IS 'Признак хрупкости товара';
COMMENT ON COLUMN warehouse_products.width IS 'Ширина товара';
COMMENT ON COLUMN warehouse_products.height IS 'Высота товара';
COMMENT ON COLUMN warehouse_products.depth IS 'Глубина товара';
COMMENT ON COLUMN warehouse_products.weight IS 'Вес товара';
COMMENT ON COLUMN warehouse_products.created_at IS 'Дата и время создания записи';
COMMENT ON COLUMN warehouse_products.updated_at IS 'Дата и время последнего обновления записи';
COMMENT ON COLUMN warehouse_products.version IS 'Версия для оптимистичной блокировки';