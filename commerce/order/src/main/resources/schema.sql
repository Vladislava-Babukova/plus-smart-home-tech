CREATE TABLE IF NOT EXISTS orders (
    order_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    shopping_cart_id UUID,
    username VARCHAR(100) NOT NULL,
    order_state VARCHAR(20) NOT NULL CHECK (order_state IN (
        'NEW', 'ON_PAYMENT', 'ON_DELIVERY', 'DONE', 'DELIVERED',
        'ASSEMBLED', 'PAID', 'COMPLETED', 'DELIVERY_FAILED',
        'ASSEMBLY_FAILED', 'PAYMENT_FAILED', 'PRODUCT_RETURNED', 'CANCELED'
    )),
    payment_id UUID,
    delivery_id UUID,
    delivery_weight DOUBLE PRECISION,
    delivery_volume DOUBLE PRECISION,
    fragile BOOLEAN DEFAULT FALSE,
    total_price NUMERIC(10,2),
    delivery_price NUMERIC(10,2),
    product_price NUMERIC(10,2),
    country VARCHAR(100),
    city VARCHAR(100),
    street VARCHAR(200),
    house VARCHAR(20),
    flat VARCHAR(20),
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    version BIGINT DEFAULT 0
);

CREATE TABLE IF NOT EXISTS order_items (
    order_item_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id UUID NOT NULL REFERENCES orders(order_id) ON DELETE CASCADE,
    product_id UUID NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    version BIGINT DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_orders_username
ON orders(username);

CREATE INDEX IF NOT EXISTS idx_orders_state
ON orders(order_state);

CREATE INDEX IF NOT EXISTS idx_orders_created_at
ON orders(created_at);

CREATE INDEX IF NOT EXISTS idx_order_items_order_id
ON order_items(order_id);

CREATE UNIQUE INDEX IF NOT EXISTS idx_order_items_order_product
ON order_items(order_id, product_id);

COMMENT ON TABLE orders IS 'Таблица для хранения заказов покупателей';
COMMENT ON COLUMN orders.order_id IS 'Уникальный идентификатор заказа';
COMMENT ON COLUMN orders.username IS 'Имя пользователя (владельца заказа)';
COMMENT ON COLUMN orders.shopping_cart_id IS 'Идентификатор корзины, из которой создан заказ';
COMMENT ON COLUMN orders.order_state IS 'Текущее состояние заказа';
COMMENT ON COLUMN orders.payment_id IS 'Идентификатор оплаты';
COMMENT ON COLUMN orders.delivery_id IS 'Идентификатор доставки';
COMMENT ON COLUMN orders.delivery_weight IS 'Общий вес заказа для доставки';
COMMENT ON COLUMN orders.delivery_volume IS 'Общий объем заказа для доставки';
COMMENT ON COLUMN orders.fragile IS 'Признак хрупкости товаров в заказе';
COMMENT ON COLUMN orders.total_price IS 'Общая стоимость заказа';
COMMENT ON COLUMN orders.delivery_price IS 'Стоимость доставки';
COMMENT ON COLUMN orders.product_price IS 'Стоимость товаров без доставки';
COMMENT ON COLUMN orders.country IS 'Страна доставки';
COMMENT ON COLUMN orders.city IS 'Город доставки';
COMMENT ON COLUMN orders.street IS 'Улица доставки';
COMMENT ON COLUMN orders.house IS 'Дом доставки';
COMMENT ON COLUMN orders.flat IS 'Квартира доставки';
COMMENT ON COLUMN orders.created_at IS 'Дата и время создания заказа';
COMMENT ON COLUMN orders.updated_at IS 'Дата и время последнего обновления заказа';
COMMENT ON COLUMN orders.version IS 'Версия для оптимистичной блокировки';

COMMENT ON TABLE order_items IS 'Таблица для хранения товаров в заказах';
COMMENT ON COLUMN order_items.order_item_id IS 'Уникальный идентификатор элемента заказа';
COMMENT ON COLUMN order_items.order_id IS 'Ссылка на заказ';
COMMENT ON COLUMN order_items.product_id IS 'Идентификатор товара';
COMMENT ON COLUMN order_items.quantity IS 'Количество товара в заказе';
COMMENT ON COLUMN order_items.version IS 'Версия для оптимистичной блокировки';