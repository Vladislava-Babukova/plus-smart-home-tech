CREATE TABLE IF NOT EXISTS payments (
    payment_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id UUID NOT NULL,
    payment_state VARCHAR(20) NOT NULL CHECK (payment_state IN ('PENDING', 'SUCCESS', 'FAILED')),
    product_cost NUMERIC(10,2),
    delivery_cost NUMERIC(10,2),
    tax_cost NUMERIC(10,2),
    total_cost NUMERIC(10,2),
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    version BIGINT DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_payments_order_id
ON payments(order_id);

CREATE INDEX IF NOT EXISTS idx_payments_state
ON payments(payment_state);

CREATE INDEX IF NOT EXISTS idx_payments_created_at
ON payments(created_at);

COMMENT ON TABLE payments IS 'Таблица для хранения информации о платежах';
COMMENT ON COLUMN payments.payment_id IS 'Уникальный идентификатор платежа';
COMMENT ON COLUMN payments.order_id IS 'Идентификатор заказа, к которому относится платеж';
COMMENT ON COLUMN payments.payment_state IS 'Статус платежа: PENDING, SUCCESS, FAILED';
COMMENT ON COLUMN payments.product_cost IS 'Стоимость товаров в заказе';
COMMENT ON COLUMN payments.delivery_cost IS 'Стоимость доставки';
COMMENT ON COLUMN payments.tax_cost IS 'Стоимость налога (НДС)';
COMMENT ON COLUMN payments.total_cost IS 'Общая стоимость заказа (товары + доставка + налог)';
COMMENT ON COLUMN payments.created_at IS 'Дата и время создания платежа';
COMMENT ON COLUMN payments.updated_at IS 'Дата и время последнего обновления платежа';
COMMENT ON COLUMN payments.version IS 'Версия для оптимистичной блокировки';