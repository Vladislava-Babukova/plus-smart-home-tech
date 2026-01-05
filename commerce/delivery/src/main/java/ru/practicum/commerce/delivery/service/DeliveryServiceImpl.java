package ru.practicum.commerce.delivery.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.practicum.commerce.delivery.dal.DeliveryRepository;
import ru.practicum.commerce.delivery.exception.DeliveryAlreadyExistsException;
import ru.practicum.commerce.delivery.exception.DeliveryNotFoundException;
import ru.practicum.commerce.delivery.exception.WarehouseServiceException;
import ru.practicum.commerce.delivery.mapper.DeliveryMapper;
import ru.practicum.commerce.delivery.model.DeliveryEntity;
import ru.yandex.practicum.commerce.contract.order.OrderClient;
import ru.yandex.practicum.commerce.contract.warehouse.WarehouseClient;
import ru.yandex.practicum.commerce.dto.delivery.DeliveryDto;
import ru.yandex.practicum.commerce.dto.delivery.DeliveryState;
import ru.yandex.practicum.commerce.dto.order.OrderDto;
import ru.yandex.practicum.commerce.dto.warehouse.AddressDto;
import ru.yandex.practicum.commerce.dto.warehouse.ShippedToDeliveryRequest;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class DeliveryServiceImpl implements DeliveryService {

    private final DeliveryRepository deliveryRepository;
    private final WarehouseClient warehouseClient;
    private final OrderClient orderClient;

    private static final BigDecimal BASE_COST = new BigDecimal("5.0");
    private static final BigDecimal FRAGILE_MULTIPLIER = new BigDecimal("0.2");
    private static final BigDecimal WEIGHT_MULTIPLIER = new BigDecimal("0.3");
    private static final BigDecimal VOLUME_MULTIPLIER = new BigDecimal("0.2");
    private static final BigDecimal ADDRESS_MULTIPLIER = new BigDecimal("0.2");
    private static final int SCALE = 2;
    private static final RoundingMode ROUNDING_MODE = RoundingMode.HALF_UP;

    @Override
    @Transactional
    public DeliveryDto createDelivery(DeliveryDto deliveryDto) {
        log.info("НАЧАЛО: Создание доставки для заказа: {}", deliveryDto.getOrderId());


        if (deliveryRepository.existsByOrderId(deliveryDto.getOrderId())) {
            log.warn("Попытка создать дублирующую доставку для заказа: {}", deliveryDto.getOrderId());
            throw new DeliveryAlreadyExistsException("Delivery already exists for order: " + deliveryDto.getOrderId());
        }


        if (log.isDebugEnabled()) {
            log.debug("Данные для создания доставки: orderId={}, fromAddress={}, toAddress={}",
                    deliveryDto.getOrderId(),
                    maskStreetForLogging(deliveryDto.getFromAddress() != null ? deliveryDto.getFromAddress().getStreet() : null),
                    maskStreetForLogging(deliveryDto.getToAddress() != null ? deliveryDto.getToAddress().getStreet() : null));
        }

        DeliveryEntity deliveryEntity = DeliveryMapper.toEntity(deliveryDto);
        DeliveryEntity savedDelivery = deliveryRepository.save(deliveryEntity);

        DeliveryDto result = DeliveryMapper.toDto(savedDelivery);

        log.info("СОЗДАНО: Доставка с ID: {} для заказа: {}",
                result.getDeliveryId(), deliveryDto.getOrderId());

        if (log.isDebugEnabled()) {
            log.debug("Созданная сущность доставки: deliveryId={}, orderId={}, state={}",
                    result.getDeliveryId(), result.getOrderId(), result.getDeliveryState());
        }

        log.info("КОНЕЦ: Успешно создана доставка для заказа: {}", deliveryDto.getOrderId());
        return result;
    }

    @Override
    @Transactional
    public BigDecimal calculateDeliveryCost(OrderDto orderDto) {
        log.info("НАЧАЛО: Расчет стоимости доставки для заказа: {}", orderDto.getOrderId());


        if (orderDto == null || orderDto.getOrderId() == null) {
            log.error("Неверные входные данные для расчета стоимости доставки");
            throw new IllegalArgumentException("OrderDto или orderId не может быть null");
        }


        log.debug("Параметры заказа для расчета доставки: orderId={}, weight={}, volume={}, fragile={}",
                orderDto.getOrderId(),
                orderDto.getDeliveryWeight(),
                orderDto.getDeliveryVolume(),
                orderDto.getFragile());

        AddressDto warehouseAddressDto;
        try {
            log.info("Запрос адреса склада для заказа: {}", orderDto.getOrderId());
            warehouseAddressDto = warehouseClient.getWarehouseAddress();

            String maskedStreet = maskStreetForLogging(warehouseAddressDto.getStreet());
            log.info("Получен адрес склада: {}", maskedStreet);

            if (log.isDebugEnabled()) {
                log.debug("Детали адреса склада: street={}, city={}",
                        maskedStreet,
                        warehouseAddressDto.getCity() != null ? warehouseAddressDto.getCity() : "не указан");
            }

        } catch (Exception e) {
            log.error("КРИТИЧЕСКАЯ ОШИБКА: Не удалось получить адрес склада. Заказ: {}. Причина: {}",
                    orderDto.getOrderId(), e.getMessage(), e);
            throw new WarehouseServiceException("Не удалось получить адрес склада для заказа: " + orderDto.getOrderId(), e);
        }

        String deliveryStreet = getDeliveryStreetFromDatabase(orderDto.getOrderId());
        log.debug("Адрес доставки из БД: {}", maskStreetForLogging(deliveryStreet));

        BigDecimal cost = calculateDeliveryCostAlgorithm(
                warehouseAddressDto.getStreet(),
                orderDto.getDeliveryWeight() != null ? orderDto.getDeliveryWeight() : 0.0,
                orderDto.getDeliveryVolume() != null ? orderDto.getDeliveryVolume() : 0.0,
                orderDto.getFragile() != null ? orderDto.getFragile() : false,
                deliveryStreet
        );

        log.info("Рассчитана стоимость доставки: {} для заказа: {}", cost, orderDto.getOrderId());

        DeliveryEntity updatedDelivery = updateDeliveryCostByOrderId(orderDto.getOrderId(), cost);


        if (log.isDebugEnabled()) {
            log.debug("Сохраненная сущность доставки: deliveryId={}, orderId={}, cost={}, state={}",
                    updatedDelivery.getDeliveryId(),
                    updatedDelivery.getOrderId(),
                    updatedDelivery.getDeliveryCost(),
                    updatedDelivery.getDeliveryState());
        }

        log.info("КОНЕЦ: Успешно завершен расчет доставки для заказа: {}", orderDto.getOrderId());
        return cost.setScale(SCALE, ROUNDING_MODE);
    }

    @Override
    @Transactional
    public void processDeliveryPicked(UUID orderId) {
        log.info("НАЧАЛО: Обработка взятия доставки для заказа: {}", orderId);

        DeliveryEntity delivery = getDeliveryByOrderIdEntity(orderId);

        log.debug("Текущий статус доставки: {}", delivery.getDeliveryState());

        ShippedToDeliveryRequest shippedRequest = ShippedToDeliveryRequest.builder()
                .orderId(orderId)
                .deliveryId(delivery.getDeliveryId())
                .build();

        log.info("Уведомление склада о доставке для заказа: {}", orderId);

        try {
            if (log.isDebugEnabled()) {
                log.debug("Отправка запроса на склад: orderId={}, deliveryId={}",
                        shippedRequest.getOrderId(), shippedRequest.getDeliveryId());
            }

            warehouseClient.shippedToDelivery(shippedRequest);

            log.debug("Склад успешно уведомлен о доставке для заказа: {}", orderId);

            delivery.setDeliveryState(DeliveryState.IN_PROGRESS);
            delivery.setUpdatedAt(LocalDateTime.now());
            deliveryRepository.save(delivery);

            log.info("Доставка для заказа {} переведена в статус IN_PROGRESS", orderId);

        } catch (Exception e) {
            log.error("ОШИБКА: Не удалось обработать взятие доставки для заказа: {}. Причина: {}",
                    orderId, e.getMessage(), e);
            delivery.setDeliveryState(DeliveryState.FAILED);
            delivery.setUpdatedAt(LocalDateTime.now());
            deliveryRepository.save(delivery);

            log.error("Доставка для заказа {} переведена в статус FAILED из-за ошибки", orderId);
            throw new WarehouseServiceException("Не удалось обработать взятие доставки для заказа: " + orderId, e);
        }

        log.info("КОНЕЦ: Обработка взятия доставки завершена для заказа: {}", orderId);
    }

    @Override
    @Transactional
    public void processDeliverySuccess(UUID orderId) {
        log.info("НАЧАЛО: Обработка успешной доставки для заказа: {}", orderId);

        DeliveryEntity delivery = getDeliveryByOrderIdEntity(orderId);

        log.debug("Текущий статус доставки перед успешной доставкой: {}", delivery.getDeliveryState());

        delivery.setDeliveryState(DeliveryState.DELIVERED);
        delivery.setUpdatedAt(LocalDateTime.now());

        deliveryRepository.save(delivery);

        log.info("Статус доставки обновлен в БД для заказа: {}", orderId);

        try {
            log.info("Обновление статуса заказа в сервисе заказов: {}", orderId);

            OrderDto updatedOrder = orderClient.delivery(orderId);

            if (updatedOrder != null) {
                log.debug("Статус заказа успешно обновлен в сервисе заказов: orderId={}", orderId);
                if (log.isDebugEnabled()) {
                    log.debug("Полученный ответ от сервиса заказов: {}", updatedOrder);
                }
            } else {
                log.warn("Сервис заказов вернул null при обновлении статуса для заказа: {}", orderId);

            }

        } catch (Exception e) {
            log.error("ОШИБКА: Не удалось обновить статус заказа в сервисе заказов: {}. Причина: {}",
                    orderId, e.getMessage(), e);
        }

        log.info("КОНЕЦ: Доставка для заказа {} успешно завершена и отмечена как DELIVERED", orderId);
    }

    @Override
    @Transactional
    public void processDeliveryFailed(UUID orderId) {
        log.info("НАЧАЛО: Обработка неудачной доставки для заказа: {}", orderId);

        DeliveryEntity delivery = getDeliveryByOrderIdEntity(orderId);

        log.debug("Текущий статус доставки перед отметкой как неудачная: {}", delivery.getDeliveryState());

        delivery.setDeliveryState(DeliveryState.FAILED);
        delivery.setUpdatedAt(LocalDateTime.now());

        deliveryRepository.save(delivery);

        log.info("Статус доставки обновлен в БД для заказа: {}", orderId);

        try {
            log.info("Уведомление сервиса заказов о неудачной доставке: {}", orderId);

            OrderDto updatedOrder = orderClient.deliveryFailed(orderId);

            if (updatedOrder != null) {
                log.debug("Сервис заказов уведомлен о неудачной доставке: orderId={}", orderId);
                if (log.isDebugEnabled()) {
                    log.debug("Полученный ответ от сервиса заказов: {}", updatedOrder);
                }
            } else {
                log.warn("Сервис заказов вернул null при уведомлении о неудачной доставке для заказа: {}", orderId);
            }

        } catch (Exception e) {
            log.error("ОШИБКА: Не удалось уведомить сервис заказов о неудачной доставке: {}. Причина: {}",
                    orderId, e.getMessage(), e);
        }

        log.info("КОНЕЦ: Доставка для заказа {} отмечена как FAILED", orderId);
    }

    @Override
    @Transactional(readOnly = true)
    public DeliveryDto getDeliveryById(UUID deliveryId) {
        log.info("Получение доставки по ID: {}", deliveryId);

        DeliveryEntity delivery = getDeliveryEntity(deliveryId);

        if (log.isDebugEnabled()) {
            log.debug("Найдена доставка: deliveryId={}, orderId={}, state={}",
                    delivery.getDeliveryId(), delivery.getOrderId(), delivery.getDeliveryState());
        }

        return DeliveryMapper.toDto(delivery);
    }

    @Override
    @Transactional(readOnly = true)
    public DeliveryDto getDeliveryByOrderId(UUID orderId) {
        log.info("Получение доставки по ID заказа: {}", orderId);

        DeliveryEntity delivery = getDeliveryByOrderIdEntity(orderId);

        if (log.isDebugEnabled()) {
            log.debug("Найдена доставка для заказа: deliveryId={}, orderId={}, state={}, cost={}",
                    delivery.getDeliveryId(), delivery.getOrderId(),
                    delivery.getDeliveryState(), delivery.getDeliveryCost());
        }

        return DeliveryMapper.toDto(delivery);
    }


    private DeliveryEntity getDeliveryEntity(UUID deliveryId) {
        log.debug("Поиск доставки по ID: {}", deliveryId);
        return deliveryRepository.findById(deliveryId)
                .orElseThrow(() -> {
                    log.error("Доставка с ID {} не найдена", deliveryId);
                    return new DeliveryNotFoundException(deliveryId);
                });
    }

    private DeliveryEntity getDeliveryByOrderIdEntity(UUID orderId) {
        log.debug("Поиск доставки по ID заказа: {}", orderId);
        return deliveryRepository.findByOrderId(orderId)
                .orElseThrow(() -> {
                    log.error("Доставка для заказа {} не найдена", orderId);
                    return new DeliveryNotFoundException("Доставка не найдена для заказа: " + orderId);
                });
    }

    private String getDeliveryStreetFromDatabase(UUID orderId) {
        log.debug("Получение адреса доставки из БД для заказа: {}", orderId);

        try {
            DeliveryEntity delivery = deliveryRepository.findByOrderId(orderId)
                    .orElseThrow(() -> new DeliveryNotFoundException("Доставка не найдена для заказа: " + orderId));

            if (delivery.getToAddress() != null && delivery.getToAddress().getStreet() != null) {
                String street = delivery.getToAddress().getStreet();
                log.debug("Найден адрес доставки: {}", maskStreetForLogging(street));
                return street;
            } else {
                log.warn("Адрес доставки не указан для заказа: {}", orderId);
                return "";
            }
        } catch (DeliveryNotFoundException e) {
            log.warn("Нет доставки для заказа: {}, невозможно получить адрес", orderId);
            throw new DeliveryNotFoundException("Доставка не найдена для заказа: " + orderId + ". Сначала создайте доставку.");
        }
    }

    private BigDecimal calculateDeliveryCostAlgorithm(String warehouseAddress,
                                                      Double weight,
                                                      Double volume,
                                                      Boolean fragile,
                                                      String deliveryStreet) {
        log.debug("Расчет стоимости доставки. Склад: {}, Адрес доставки: {}, Вес: {}, Объем: {}, Хрупкое: {}",
                maskStreetForLogging(warehouseAddress),
                maskStreetForLogging(deliveryStreet),
                weight, volume, fragile);

        BigDecimal cost = BASE_COST;
        log.debug("Базовая стоимость: {}", cost);

        BigDecimal addressMultiplier;
        if (warehouseAddress.contains("ADDRESS_1")) {
            addressMultiplier = BigDecimal.ONE;
        } else if (warehouseAddress.contains("ADDRESS_2")) {
            addressMultiplier = new BigDecimal("2");
        } else {
            addressMultiplier = BigDecimal.ONE;
        }

        cost = cost.multiply(addressMultiplier).add(BASE_COST);
        log.debug("Стоимость после учета множителя адреса склада ({}): {}", addressMultiplier, cost);

        if (fragile != null && fragile) {
            BigDecimal fragileCost = cost.multiply(FRAGILE_MULTIPLIER);
            cost = cost.add(fragileCost);
            log.debug("Добавлена стоимость за хрупкость: {}", fragileCost);
        }

        if (weight != null) {
            BigDecimal weightCost = BigDecimal.valueOf(weight).multiply(WEIGHT_MULTIPLIER);
            cost = cost.add(weightCost);
            log.debug("Добавлена стоимость за вес ({} кг): {}", weight, weightCost);
        }

        if (volume != null) {
            BigDecimal volumeCost = BigDecimal.valueOf(volume).multiply(VOLUME_MULTIPLIER);
            cost = cost.add(volumeCost);
            log.debug("Добавлена стоимость за объем ({} м³): {}", volume, volumeCost);
        }

        if (deliveryStreet != null && !deliveryStreet.isEmpty() && !deliveryStreet.equals(warehouseAddress)) {
            BigDecimal addressCost = cost.multiply(ADDRESS_MULTIPLIER);
            cost = cost.add(addressCost);
            log.debug("Добавлена стоимость за удаленность адреса доставки: {}", addressCost);
        }

        log.debug("Итоговая стоимость доставки: {}", cost);
        return cost;
    }

    private DeliveryEntity updateDeliveryCostByOrderId(UUID orderId, BigDecimal deliveryCost) {
        log.info("Обновление стоимости доставки для заказа: {} на {}", orderId, deliveryCost);

        DeliveryEntity delivery = getDeliveryByOrderIdEntity(orderId);

        log.debug("Текущая стоимость доставки до обновления: {}", delivery.getDeliveryCost());

        delivery.setDeliveryCost(deliveryCost);
        delivery.setUpdatedAt(LocalDateTime.now());

        DeliveryEntity updatedDelivery = deliveryRepository.save(delivery);

        log.info("Стоимость доставки обновлена для заказа: {}", orderId);
        log.debug("Новая стоимость доставки: {}", updatedDelivery.getDeliveryCost());

        return updatedDelivery;
    }

    private String maskStreetForLogging(String street) {
        if (street == null || street.trim().isEmpty()) {
            return "[не указано]";
        }

        String trimmed = street.trim();
        if (trimmed.length() <= 3) {
            return trimmed;
        }

        return trimmed.substring(0, 3) + "***";
    }
}