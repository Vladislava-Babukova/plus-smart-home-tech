package ru.yandex.practicum.telemetry.analyzer.dal.model;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;

@Entity
@Getter
@Setter
@Table(name = "scenarios")
public class Scenario {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "hub_id")
    private String hubId;

    private String name;

    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(
            name = "scenario_conditions",
            joinColumns = @JoinColumn(name = "scenario_id"),
            uniqueConstraints = @UniqueConstraint(columnNames = {"scenario_id", "sensor_id", "condition_id"})
    )
    @MapKeyColumn(name = "sensor_id")
    @Column(name = "condition_id")
    private Map<String, Long> conditionIds = new HashMap<>();

    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(
            name = "scenario_actions",
            joinColumns = @JoinColumn(name = "scenario_id"),
            uniqueConstraints = @UniqueConstraint(columnNames = {"scenario_id", "sensor_id", "action_id"})
    )
    @MapKeyColumn(name = "sensor_id")
    @Column(name = "action_id")
    private Map<String, Long> actionIds = new HashMap<>();
}