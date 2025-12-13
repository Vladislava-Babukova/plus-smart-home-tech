CREATE TABLE IF NOT EXISTS shopping_carts (
    shopping_cart_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    username VARCHAR(100) NOT NULL,
    cart_state VARCHAR(20) NOT NULL CHECK (cart_state IN ('ACTIVE', 'DEACTIVATED')),
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);


CREATE TABLE IF NOT EXISTS shopping_cart_items (
    cart_item_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    shopping_cart_id UUID NOT NULL REFERENCES shopping_carts(shopping_cart_id) ON DELETE CASCADE,
    product_id UUID NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    version BIGINT DEFAULT 0
);


CREATE UNIQUE INDEX IF NOT EXISTS idx_shopping_carts_username_active
ON shopping_carts(username) WHERE cart_state = 'ACTIVE';

CREATE INDEX IF NOT EXISTS idx_shopping_cart_items_cart_id
ON shopping_cart_items(shopping_cart_id);

CREATE UNIQUE INDEX IF NOT EXISTS idx_shopping_cart_items_cart_product
ON shopping_cart_items(shopping_cart_id, product_id);


COMMENT ON TABLE shopping_carts IS 'Таблица для хранения корзин покупателей';
COMMENT ON COLUMN shopping_carts.shopping_cart_id IS 'Уникальный идентификатор корзины';
COMMENT ON COLUMN shopping_carts.username IS 'Имя пользователя (владельца корзины)';
COMMENT ON COLUMN shopping_carts.cart_state IS 'Состояние корзины: ACTIVE, DEACTIVATED';
COMMENT ON COLUMN shopping_carts.created_at IS 'Дата и время создания корзины';
COMMENT ON COLUMN shopping_carts.updated_at IS 'Дата и время последнего обновления корзины';

COMMENT ON TABLE shopping_cart_items IS 'Таблица для хранения товаров в корзинах';
COMMENT ON COLUMN shopping_cart_items.cart_item_id IS 'Уникальный идентификатор элемента корзины';
COMMENT ON COLUMN shopping_cart_items.shopping_cart_id IS 'Ссылка на корзину';
COMMENT ON COLUMN shopping_cart_items.product_id IS 'Идентификатор товара';
COMMENT ON COLUMN shopping_cart_items.quantity IS 'Количество товара в корзине';
COMMENT ON COLUMN shopping_cart_items.version IS 'Версия для оптимистичной блокировки';