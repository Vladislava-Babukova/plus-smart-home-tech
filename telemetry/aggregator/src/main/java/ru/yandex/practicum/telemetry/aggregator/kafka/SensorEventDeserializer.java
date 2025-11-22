package ru.yandex.practicum.telemetry.aggregator.kafka;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.io.Decoder;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

import java.io.IOException;
import java.util.Map;

public class SensorEventDeserializer implements Deserializer<SensorEventAvro> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // Nothing to configure
    }

    @Override
    public SensorEventAvro deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            SpecificDatumReader<SensorEventAvro> reader = new SpecificDatumReader<>(SensorEventAvro.getClassSchema());
            Decoder decoder = DecoderFactory.get().binaryDecoder(data, null);
            return reader.read(null, decoder);
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize SensorEventAvro", e);
        }
    }

    @Override
    public void close() {
        // Nothing to close
    }
}
