package ch.mobility.kafka;

import ch.mobility.charger2mob.MobOCPPShutdownNotification;
import ch.mobility.charger2mob.MobOCPPStartupNotification;
import io.apicurio.registry.serde.SerdeConfig;
import io.apicurio.registry.serde.avro.AvroKafkaDeserializer;
import io.apicurio.registry.serde.avro.AvroKafkaSerdeConfig;
import io.apicurio.registry.serde.config.IdOption;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class AvroConsumer<T extends GenericRecord> {

    final static int MAX_EXCEPTIONS = 3;
    private static Duration duration = Duration.ofMillis(100);

    private static Logger LOGGER = LoggerFactory.getLogger(AvroConsumer.class);

    private final ConsumerThread consumerThread;

    public AvroConsumer(
            String bootstrapServerValue,
            String registryUrlValue,
            String topicnameValue,
            String consumerGroupIdValue,
            IncomingKafkaMessageCallback incomingKafkaMessageCallbackValue) {
        this(bootstrapServerValue, registryUrlValue, topicnameValue, consumerGroupIdValue, incomingKafkaMessageCallbackValue, null);
    }

    public AvroConsumer(
            String bootstrapServerValue,
            String registryUrlValue,
            String topicnameValue,
            String consumerGroupIdValue,
            IncomingKafkaMessageCallback incomingKafkaMessageCallbackValue,
            MobOCPPStartupShutdownKafkaMessageCallback mobOCPPStartupShutdownKafkaMessageCallbackValue) {
        consumerThread = new ConsumerThread(bootstrapServerValue, registryUrlValue, topicnameValue, consumerGroupIdValue, incomingKafkaMessageCallbackValue, mobOCPPStartupShutdownKafkaMessageCallbackValue);
        new Thread(consumerThread).start();
    }

    public void close() {
        consumerThread.stop();
    }

    private static class ConsumerThread<T extends GenericRecord> implements Runnable {

        private final String bootstrapServer;
        private final String registryUrl;
        private final String topicname;
        private final String consumerGroupId;
        private final MobOCPPStartupShutdownKafkaMessageCallback mobOCPPStartupShutdownKafkaMessageCallback;
        private final IncomingKafkaMessageCallback incomingKafkaMessageCallback;
        private Consumer<String, GenericRecord> consumer = null;
        private boolean run = true;

        ConsumerThread(
                String bootstrapServerValue,
                String registryUrlValue,
                String topicnameValue,
                String consumerGroupIdValue,
                IncomingKafkaMessageCallback incomingKafkaMessageCallbackValue,
                MobOCPPStartupShutdownKafkaMessageCallback mobOCPPStartupShutdownKafkaMessageCallbackValue
        ) {
            if (bootstrapServerValue == null || "".equals(bootstrapServerValue)) throw new IllegalArgumentException("bootstrapServerValue");
            if (registryUrlValue == null || "".equals(registryUrlValue)) throw new IllegalArgumentException("registryUrlValue");
            if (topicnameValue == null || "".equals(topicnameValue)) throw new IllegalArgumentException("topicnameValue");
            if (consumerGroupIdValue == null || "".equals(consumerGroupIdValue)) throw new IllegalArgumentException("consumerGroupIdValue");
            if (incomingKafkaMessageCallbackValue == null) throw new IllegalArgumentException("incomingKafkaMessageCallback");
            this.bootstrapServer = bootstrapServerValue;
            this.registryUrl = registryUrlValue;
            this.topicname = topicnameValue;
            this.consumerGroupId = consumerGroupIdValue;
            this.incomingKafkaMessageCallback = incomingKafkaMessageCallbackValue;
            this.mobOCPPStartupShutdownKafkaMessageCallback = mobOCPPStartupShutdownKafkaMessageCallbackValue;
        }

        private Consumer<String, GenericRecord> createConsumer() {
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AvroKafkaDeserializer.class);
            props.put(SerdeConfig.REGISTRY_URL, registryUrl);
            //props.put("schema.registry.url", registryUrl);
            props.put(SerdeConfig.USE_ID, IdOption.globalId.name());
            props.put(AvroKafkaSerdeConfig.USE_SPECIFIC_AVRO_READER, Boolean.TRUE);
//            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

            Consumer<String, GenericRecord> consumer = new KafkaConsumer<String, GenericRecord>(props);
            consumer.subscribe(Arrays.asList(topicname));

            LOGGER.info("AvroConsumer subscribed to topic '" + topicname + "' on host '" + bootstrapServer + "'");
            return consumer;
        }

        void stop() {
            run = false;
        }

        @Override
        public void run() {

            try {
                int numberOfExceptions = 0;
                while (run) {

                    while (run && consumer == null) {
                        if (KafkaHelper.isKafkaAvailable(bootstrapServer)) {
                            consumer = createConsumer();
                            LOGGER.debug("AvroConsumer start...");
                        } else {
                            LOGGER.warn("Kafka is not available, AvroConsumer not connected, will try again...");
                            try { Thread.sleep(1000l); } catch (InterruptedException e) {}
                        }
                    }

                    try {
                        final ConsumerRecords<String, GenericRecord> records = consumer.poll(duration);
                        for (ConsumerRecord<String, GenericRecord> record : records) {
                            LOGGER.debug("record.value().getClass(): " + record.value().getClass());
                            if (record.value().getClass().isAssignableFrom(MobOCPPStartupNotification.class)) {
                                if (mobOCPPStartupShutdownKafkaMessageCallback != null) {
                                    mobOCPPStartupShutdownKafkaMessageCallback.processMobOCPPStartupNotification((MobOCPPStartupNotification)record.value());
                                }
                            } else if (record.value().getClass().isAssignableFrom(MobOCPPShutdownNotification.class)) {
                                if (mobOCPPStartupShutdownKafkaMessageCallback != null) {
                                    mobOCPPStartupShutdownKafkaMessageCallback.processMobOCPPShutdownNotification((MobOCPPShutdownNotification)record.value());
                                }
                            } else {
                                incomingKafkaMessageCallback.processIncomingKafkaMessage(record.value());
                            }
                        }
                        numberOfExceptions = 0;
                    } catch (Exception e) {
                        LOGGER.error(e.getClass().getSimpleName() + ": " + e.getMessage(), e);
                        consumer = null;
                        numberOfExceptions++;
                        if (numberOfExceptions > MAX_EXCEPTIONS) {
                            run = false;
                            LOGGER.error("Too many exceptions, stopping Consumer-Thread !!!!");
                        }
                        // TOOD Braucht es das? Falls Ja, dann neuen Consumer erstellt !!!!
//                        if (consumer != null) {
//                            consumer.close();
//                            consumer = null;
//                        }
                    }
                }
            } finally {
                if (consumer != null) {
                    consumer.close();
                }
            }
            LOGGER.debug("Consumer stop");
        }
    }
}
