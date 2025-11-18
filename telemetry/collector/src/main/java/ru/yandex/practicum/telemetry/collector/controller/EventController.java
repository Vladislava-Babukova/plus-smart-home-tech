package ru.yandex.practicum.telemetry.collector.controller;

import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;
import ru.yandex.practicum.grpc.telemetry.collector.CollectorControllerGrpc;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.telemetry.collector.model.hub.HubEventType;
import ru.yandex.practicum.telemetry.collector.model.sensor.SensorEventType;
import ru.yandex.practicum.telemetry.collector.service.EventService;
import ru.yandex.practicum.telemetry.collector.model.sensor.SensorEvent;
import ru.yandex.practicum.telemetry.collector.model.hub.HubEvent;

@Slf4j
@GrpcService
@RequiredArgsConstructor
public class EventController extends CollectorControllerGrpc.CollectorControllerImplBase {
    private final EventService eventService;

    @Override
    public void collectSensorEvent(SensorEventProto request, StreamObserver<Empty> responseObserver) {
        try {
            log.info("получено gRPC событие датчика, id={}", request.getId());
            log.debug("SensorEventProto = {}", request);

            // Преобразование protobuf -> domain model
            SensorEvent domainEvent = convertToSensorEvent(request);
            eventService.processSensorEvent(domainEvent);

            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Ошибка обработки события датчика", e);
            responseObserver.onError(new StatusRuntimeException(
                    Status.INTERNAL
                            .withDescription(e.getLocalizedMessage())
                            .withCause(e)
            ));
        }
    }

    @Override
    public void collectHubEvent(HubEventProto request, StreamObserver<Empty> responseObserver) {
        try {
            log.info("получено gRPC событие хаба, hubId={}", request.getHubId());
            log.debug("HubEventProto = {}", request);

            // Преобразование protobuf -> domain model
            HubEvent domainEvent = convertToHubEvent(request);
            eventService.processHubEvent(domainEvent);

            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Exception e) {
            log.error("Ошибка обработки события хаба", e);
            responseObserver.onError(new StatusRuntimeException(
                    Status.INTERNAL
                            .withDescription(e.getLocalizedMessage())
                            .withCause(e)
            ));
        }
    }

    private SensorEvent convertToSensorEvent(SensorEventProto proto) {
        SensorEvent event = new SensorEvent() {
            @Override
            public SensorEventType getType() {
                return null;
            }
        };
        event.setId(proto.getId());
        return event;
    }

    private HubEvent convertToHubEvent(HubEventProto proto) {

        HubEvent event = new HubEvent() {
            @Override
            public HubEventType getType() {
                return null;
            }
        };
        event.setHubId(proto.getHubId());

        return event;
    }
}