package ru.yandex.practicum.telemetry.analyzer.dal.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.yandex.practicum.telemetry.analyzer.dal.model.Action;

public interface ActionRepository extends JpaRepository<Action, Long> {
}
