package ru.yandex.practicum.telemetry.collector.service.hub.mapper;

import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.ScenarioConditionProto;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.util.List;
import java.util.stream.Collectors;

public class ClassToAvroMapper {

    public static List<ScenarioConditionAvro> mapConditionsToAvro(List<ScenarioConditionProto> conditions) {
        return conditions.stream()
                .map(ClassToAvroMapper::mapConditionToAvro)
                .collect(Collectors.toList());
    }

    private static ScenarioConditionAvro mapConditionToAvro(ScenarioConditionProto condition) {
        ScenarioConditionAvro.Builder builder = ScenarioConditionAvro.newBuilder()
                .setSensorId(condition.getSensorId())
                .setType(ConditionTypeAvro.valueOf(condition.getType().name()))
                .setOperation(ConditionOperationAvro.valueOf(condition.getOperation().name()));

        // Обработка oneof value - преобразование в Integer
        if (condition.hasBoolValue()) {
            // Преобразуем boolean в Integer (1 для true, 0 для false)
            builder.setValue(condition.getBoolValue() ? 1 : 0);
        } else if (condition.hasIntValue()) {
            builder.setValue(condition.getIntValue());
        } else {
            builder.setValue(null);  // VALUE_NOT_SET
        }

        return builder.build();
    }

    public static List<DeviceActionAvro> mapActionsToAvro(List<DeviceActionProto> actions) {
        return actions.stream()
                .map(ClassToAvroMapper::mapActionToAvro)
                .collect(Collectors.toList());
    }

    private static DeviceActionAvro mapActionToAvro(DeviceActionProto action) {
        DeviceActionAvro.Builder builder = DeviceActionAvro.newBuilder()
                .setSensorId(action.getSensorId())
                .setType(ActionTypeAvro.valueOf(action.getType().name()));

        // Для union {null, int} - явно передаем null когда значение не нужно
        if (action.getValue() != 0) {
            builder.setValue(action.getValue());  // int значение
        } else {
            builder.setValue(null);  // null из union
        }

        return builder.build();
    }
}