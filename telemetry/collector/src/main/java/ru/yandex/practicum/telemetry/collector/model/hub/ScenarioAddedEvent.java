package ru.yandex.practicum.telemetry.collector.model.hub;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.List;

@Getter
@Setter
@ToString
@Builder
@Deprecated
public class ScenarioAddedEvent extends HubEvent {
    @NotBlank
    @Size(min = 3, max = 2147483647, message = "Длина должна быть от 3 до 2147483647 символов")
    private String name;
    @NotNull
    private List<ScenarioCondition> conditions;
    @NotNull
    private List<DeviceAction> actions;

    @Override
    public HubEventType getType() {
        return HubEventType.SCENARIO_ADDED;
    }
}
