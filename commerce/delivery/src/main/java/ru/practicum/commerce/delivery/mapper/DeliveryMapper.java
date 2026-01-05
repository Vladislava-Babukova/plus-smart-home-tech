package ru.practicum.commerce.delivery.mapper;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import ru.practicum.commerce.delivery.model.DeliveryEntity;
import ru.yandex.practicum.commerce.dto.delivery.DeliveryDto;
import ru.yandex.practicum.commerce.dto.warehouse.AddressDto;

import java.util.UUID;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DeliveryMapper {

    public static DeliveryDto toDto(DeliveryEntity entity) {
        if (entity == null) {
            return null;
        }

        return DeliveryDto.builder()
                .deliveryId(entity.getDeliveryId())
                .orderId(entity.getOrderId())
                .deliveryState(entity.getDeliveryState())
                .fromAddress(entity.getFromAddress() != null ?
                        convertAddressToDto(entity.getFromAddress()) : null)
                .toAddress(entity.getToAddress() != null ?
                        convertAddressToDto(entity.getToAddress()) : null)
                .build();
    }

    public static DeliveryEntity toEntity(DeliveryDto dto) {
        if (dto == null) {
            return null;
        }

        return DeliveryEntity.builder()
                .deliveryId(dto.getDeliveryId())
                .orderId(dto.getOrderId())
                .deliveryState(dto.getDeliveryState())
                .fromAddress(dto.getFromAddress() != null ?
                        convertDtoToAddress(dto.getFromAddress()) : null)
                .toAddress(dto.getToAddress() != null ?
                        convertDtoToAddress(dto.getToAddress()) : null)
                .build();
    }

    public static DeliveryEntity createNewDelivery(UUID orderId,
                                                   DeliveryEntity.Address fromAddress,
                                                   DeliveryEntity.Address toAddress,
                                                   Double weight,
                                                   Double volume,
                                                   Boolean fragile) {
        return DeliveryEntity.builder()
                .orderId(orderId)
                .fromAddress(fromAddress)
                .toAddress(toAddress)
                .deliveryWeight(weight)
                .deliveryVolume(volume)
                .fragile(fragile != null ? fragile : false)
                .build();
    }

    public static AddressDto convertAddressToDto(DeliveryEntity.Address address) {
        if (address == null) {
            return null;
        }

        return AddressDto.builder()
                .country(address.getCountry())
                .city(address.getCity())
                .street(address.getStreet())
                .house(address.getHouse())
                .flat(address.getFlat())
                .build();
    }

    public static DeliveryEntity.Address convertDtoToAddress(AddressDto dto) {
        if (dto == null) {
            return null;
        }

        return DeliveryEntity.Address.builder()
                .country(dto.getCountry())
                .city(dto.getCity())
                .street(dto.getStreet())
                .house(dto.getHouse())
                .flat(dto.getFlat())
                .build();
    }
}