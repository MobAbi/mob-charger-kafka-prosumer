package ch.mobility.kafka;

import org.apache.avro.generic.GenericRecord;

public interface IncomingKafkaMessageCallback<T extends GenericRecord> {
    void processIncomingKafkaMessage(T message);
}
