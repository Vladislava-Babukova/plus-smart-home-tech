package ru.yandex.practicum.telemetry.collector.model.sensor;

import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@Builder
@Deprecated
public class MotionSensorEvent extends SensorEvent {
    @NotNull
    private int linkQuality;
    @NotNull
    private Boolean motion;
    @NotNull
    private int voltage;

    @Override
    public SensorEventType getType() {
        return SensorEventType.MOTION_SENSOR_PROTO;
    }
}