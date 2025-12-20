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

CREATE TABLE IF NOT EXISTS order_bookings (
    order_booking_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id UUID NOT NULL,
    delivery_id UUID,
    product_id UUID NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    version BIGINT DEFAULT 0
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_warehouse_products_product_id
ON warehouse_products(product_id);

CREATE INDEX IF NOT EXISTS idx_warehouse_products_quantity
ON warehouse_products(quantity);

CREATE INDEX IF NOT EXISTS idx_order_bookings_order_id
ON order_bookings(order_id);

CREATE INDEX IF NOT EXISTS idx_order_bookings_delivery_id
ON order_bookings(delivery_id);

CREATE INDEX IF NOT EXISTS idx_order_bookings_product_id
ON order_bookings(product_id);

CREATE INDEX IF NOT EXISTS idx_order_bookings_order_product
ON order_bookings(order_id, product_id);

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

COMMENT ON TABLE order_bookings IS 'Таблица для хранения информации о забронированных товарах для заказов';
COMMENT ON COLUMN order_bookings.order_booking_id IS 'Уникальный идентификатор бронирования';
COMMENT ON COLUMN order_bookings.order_id IS 'Идентификатор заказа';
COMMENT ON COLUMN order_bookings.delivery_id IS 'Идентификатор доставки';
COMMENT ON COLUMN order_bookings.product_id IS 'Идентификатор товара';
COMMENT ON COLUMN order_bookings.quantity IS 'Количество забронированного товара';
COMMENT ON COLUMN order_bookings.created_at IS 'Дата и время создания бронирования';
COMMENT ON COLUMN order_bookings.updated_at IS 'Дата и время последнего обновления бронирования';
COMMENT ON COLUMN order_bookings.version IS 'Версия для оптимистичной блокировки';