package ru.yandex.practicum.telemetry.analyzer.dal.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

@Entity
@Getter
@Setter
@Table(name = "sensors")
public class Sensor {
    @Id
    private String id;

    @Column(name = "hub_id")
    private String hubId;
}
