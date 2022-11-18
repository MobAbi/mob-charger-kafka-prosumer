package ch.mobility.kafka;

import ch.mobility.mob2charger.*;
import io.apicurio.registry.serde.SerdeConfig;
import io.apicurio.registry.serde.avro.AvroKafkaSerdeConfig;
import io.apicurio.registry.serde.avro.AvroKafkaSerializer;
import io.apicurio.registry.serde.config.IdOption;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

public class AvroProducer {

    private static Logger LOGGER = LoggerFactory.getLogger(AvroProducer.class);

    private static String bootstrapServer;
    private static String registryUrl;
    private static String topicname;

    public static void init(String bootstrapServerValue, String registryUrlValue, String topicnameValue) {
        if (bootstrapServerValue == null || "".equals(bootstrapServerValue)) throw new IllegalArgumentException("bootstrapServerValue");
        if (registryUrlValue == null || "".equals(registryUrlValue)) throw new IllegalArgumentException("registryUrlValue");
        if (topicnameValue == null || "".equals(topicnameValue)) throw new IllegalArgumentException("topicnameValue");
        bootstrapServer = bootstrapServerValue;
        registryUrl = registryUrlValue;
        topicname = topicnameValue;
    }

    private static AvroProducer INSTANCE = null;

    public static AvroProducer get() {
        if (INSTANCE == null) {
            INSTANCE = new AvroProducer();
        }
        return INSTANCE;
    }

    private Producer producer;

    public AvroProducer() {
        producer = createProducer();
    }

    private Producer createProducer() {
        if (KafkaHelper.isKafkaAvailable(bootstrapServer)) {
            Properties props = new Properties();

            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AvroKafkaSerializer.class);
            props.put(SerdeConfig.REGISTRY_URL, registryUrl);
            props.put(SerdeConfig.USE_ID, IdOption.globalId.name());
            props.put(AvroKafkaSerdeConfig.USE_SPECIFIC_AVRO_READER, Boolean.TRUE);
            props.put(SerdeConfig.AUTO_REGISTER_ARTIFACT, Boolean.TRUE);

            final Producer producer = new KafkaProducer(props);
            LOGGER.info("AvroProducer Topic: " + topicname + " on host " + bootstrapServer);
            return producer;
        }
        LOGGER.warn("Kafka is not available, AvroProducer not connected ...");
        return null;
    }

    public void close() {
        if (producer != null) {
            producer.flush();
            producer.close();
            producer = null;
        }
    }

    public void send(GenericRecord genericRecord) {
        if (genericRecord != null) {
            if (producer == null) {
                producer = createProducer();
            }

            if (producer == null) {
                LOGGER.error("AvroProducer not connected, Record is NOT send: " + genericRecord);
            } else {
                ProducerRecord<String, GenericRecord> record = new ProducerRecord<>(topicname, genericRecord);
                try {
                    final Future<RecordMetadata> future = producer.send(record);
                    try {
                        final RecordMetadata rmd = future.get();
                        LOGGER.trace("Kafka RecordMetadata Offset: " + rmd.offset());
                        LOGGER.trace("Kafka RecordMetadata Timestamp: " + rmd.timestamp());
                    } catch (InterruptedException e) {
                        LOGGER.error("Future<RecordMetadata>.get() Exception: " + e.getMessage(), e);
                    } catch (ExecutionException e) {
                        if (e.getCause() instanceof TimeoutException) {
                            LOGGER.error("Future<RecordMetadata>.get() TimeoutException: " + e.getMessage(), e);
                        } else {
                            LOGGER.error("Future<RecordMetadata>.get() Exception: " + e.getMessage(), e);
                        }
                    }
                    LOGGER.info("Gesendet: " + genericRecord.getClass().getSimpleName() + "=" + genericRecord);
                } catch (Exception e) {
                    LOGGER.error("AvroProducer Error: " + (e.getCause() == null ? e.getMessage() : e.getCause().getMessage()), e);
                    producer = null;
                    throw e;
                }
            }
        }
    }

    private static CSRequest request(String messageId) {
        CSRequest csRequest = CSRequest.newBuilder()
                .setMessageId(messageId)
                .setRequestCreatedAt(DateTimeHelper.format(Instant.now()))
                .build();
        return csRequest;
    }

    public void requestStatusConnected(String messageId) {
        CSStatusConnectedRequest request = CSStatusConnectedRequest.newBuilder()
                .setRequestInfo(request(messageId))
                .build();
        send(request);
    }

    public void requestRecentlyConnected(String messageId, Integer daysOfHistoryData) {
        CSRecentlyConnectedRequest request = CSRecentlyConnectedRequest.newBuilder()
                .setRequestInfo(request(messageId))
                .setDaysOfHistoryData(daysOfHistoryData)
                .build();
        send(request);
    }

    public void requestStatusForId(String messageId, String id, Integer connectorId, Integer daysOfHistoryData) {
        CSStatusForIdRequest request = CSStatusForIdRequest.newBuilder()
                .setRequestInfo(request(messageId))
                .setId(id)
                .setConnectorId(getIntegerAsInteger(connectorId, 1))
                .setDaysOfHistoryData(daysOfHistoryData)
                .build();
        send(request);
    }

    public void requestStart(String messageId, String id, Integer connectorId, String tagId, Integer maxCurrent, Integer numberOfPhases) {
        CSStartChargingRequest request = CSStartChargingRequest.newBuilder()
                .setRequestInfo(request(messageId))
                .setId(id)
                .setConnectorId(getIntegerAsInt(connectorId, 1))
                .setTagId(tagId)
                .setMaxCurrent(maxCurrent)
                .setNumberOfPhases(numberOfPhases)
                .build();
        send(request);
    }

    public void requestStop(String messageId, String id, Integer connectorId) {
        CSStopChargingRequest request = CSStopChargingRequest.newBuilder()
                .setRequestInfo(request(messageId))
                .setId(id)
                .setConnectorId(getIntegerAsInteger(connectorId, 1))
                .build();
        send(request);
    }

    public void requestReset(String messageId, String id) {
        CSResetRequest request = CSResetRequest.newBuilder()
                .setRequestInfo(request(messageId))
                .setId(id)
                .build();
        send(request);
    }

    public void requestUnlock(String messageId, String id, Integer connectorId) {
        CSUnlockRequest request = CSUnlockRequest.newBuilder()
                .setRequestInfo(request(messageId))
                .setId(id)
                .setConnectorId(getIntegerAsInt(connectorId, 1))
                .build();
        send(request);
    }

    public void requestProfile(String messageId, String id, Integer connectorId, Integer maxCurrent, Integer numberOfPhases) {
        CSChangeChargingCurrentRequest request = CSChangeChargingCurrentRequest.newBuilder()
                .setRequestInfo(request(messageId))
                .setId(id)
                .setConnectorId(getIntegerAsInt(connectorId, 1))
                .setMaxCurrent(getIntegerAsInt(maxCurrent, 60))
                .setNumberOfPhases(getIntegerAsInteger(numberOfPhases, 3))
                .build();
        send(request);
    }

    public void requestTrigger(String messageId, String id, Integer connectorId, String trigger) {
        CSTriggerRequest request = CSTriggerRequest.newBuilder()
                .setRequestInfo(request(messageId))
                .setId(id)
                .setConnectorId(getIntegerAsInt(connectorId, 1))
                .setTriggertype(CPTriggerKeywordsV1XEnum.valueOf(trigger.toUpperCase()))
                .build();
        send(request);
    }

    public void requestChangeOperationalStatus(String messageId, String id, Integer connectorId, boolean isOperative) {
        CSOperationalStatusRequest request = CSOperationalStatusRequest.newBuilder()
                .setRequestInfo(request(messageId))
                .setId(id)
                .setConnectorId(getIntegerAsInt(connectorId, 1))
                .setOperationalStatus( isOperative ? CPOperationalStatusEnum.OPERATIVE : CPOperationalStatusEnum.INOPERATIVE )
                .build();
        send(request);
    }

    private Integer getIntegerAsInteger(Integer integerValue, int defaultValue) {
        return Integer.valueOf(getIntegerAsInt(integerValue, defaultValue));
    }

    private int getIntegerAsInt(Integer integerValue, int defaultValue) {
        if (integerValue != null) {
            if (integerValue.intValue() < 0) {
                throw new IllegalArgumentException("Integer must be greater then 0: " + integerValue);
            }
            return integerValue.intValue();
        }
        return defaultValue;
    }
}
