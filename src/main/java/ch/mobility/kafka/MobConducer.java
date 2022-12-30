package ch.mobility.kafka;

import ch.mobility.mob2charger.CSOperationalStatusRequest;
import ch.mobility.mob2charger.CSStatusConnectedRequest;
import ch.mobility.charger2mob.*;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class MobConducer {

    private static class IncomingKafkaMessageCallbackImpl implements IncomingKafkaMessageCallback {

        private List<GenericRecord> records = new ArrayList<>();

        private CSResponse createErrorCSResponse(String messageId, String error) {
            return CSResponse.newBuilder()
                    .setMessageId(messageId)
                    .setError(error)
                    .setOCPPBackendId("MobConducer-001")
                    .setResponseCreatedAt(DateTimeHelper.format(Instant.now()))
                    .build();
        }

        @Override
        public void processIncomingKafkaMessage(GenericRecord recordIncoming) {
            if (recordIncoming != null) {
               GenericRecord recordOutgoing = null;
               if (recordIncoming instanceof CSStatusConnectedRequest) {
                   final CSStatusConnectedRequest request = (CSStatusConnectedRequest) recordIncoming;
                   recordOutgoing = CSStatusConnectedResponse.newBuilder()
                           .setResponseInfo(createErrorCSResponse(request.getRequestInfo().getMessageId(), "Not implemented yet"))
                           .setCSStatusList(new ArrayList<>())
                           .build();
               } else if (recordIncoming instanceof CSOperationalStatusRequest) {
                   final CSOperationalStatusRequest request = (CSOperationalStatusRequest) recordIncoming;
                   recordOutgoing = CSOperationalStatusResponse.newBuilder()
                           .setResponseInfo(createErrorCSResponse(request.getRequestInfo().getMessageId(), "Not implemented yet"))
                           .setId(request.getId())
                           .setConnectorId(request.getConnectorId())
                           .setBackendStatus(CSBackendStatusEnum.CS_CONNECTED)
                           .setRequestResult(CSRequestResultEnum.Rejected)
                           .build();
               } else {
                   LOGGER.error("Unsupported Request: " + recordIncoming);
               }

               if (recordOutgoing != null) {
                   synchronized (records) {
                       records.add(recordOutgoing);
                   }
               }
            }
        }

        public GenericRecord nextMessage() {
            synchronized (records) {
                GenericRecord record = null;
                if (!records.isEmpty()) {
                    record = records.remove(0);
                }
                return record;
            }
        }
    }

    private static Logger LOGGER = LoggerFactory.getLogger(MobConducer.class);

    public static void main(String[] args) throws Exception {

        // Check arguments length value
        if (args.length != 2){
            LOGGER.info("MobConducer <bootstrapServer> <registryUrl>");
            return;
        }

        final String bootstrapServer = args[0];
        final String registryUrl = args[1];

        validate(bootstrapServer, registryUrl);

        IncomingKafkaMessageCallbackImpl incomingKafkaMessageCallback = new IncomingKafkaMessageCallbackImpl();

        final AvroConsumer avroConsumer = new AvroConsumer(bootstrapServer, registryUrl, TopicnameHelper.MOB2CHARGER, "MobConducer", incomingKafkaMessageCallback);
        final AvroProducer avroProducer = new AvroProducer(bootstrapServer, registryUrl, TopicnameHelper.CHARGER2MOB);

        try {
            boolean run = true;
            while (run) {
                GenericRecord record = incomingKafkaMessageCallback.nextMessage();
                if (record == null) {
                    Thread.sleep(50L);
                } else {
                    avroProducer.send(record);
                }
            }
        } finally {
            avroProducer.close();
            avroConsumer.close();
        }
    }

    private static void validate(String bootstrapServer, String registryUrl) {
        final int minlength = 10;
        if (bootstrapServer == null || "".equals(bootstrapServer)) {
            throw new IllegalArgumentException("<bootstrapServer> missing");
        }
        if (registryUrl == null || "".equals(registryUrl)) {
            throw new IllegalArgumentException("<registryUrl> missing");
        }
        if (bootstrapServer.length() < minlength) {
            LOGGER.warn("<bootstrapServer> is less then " + minlength + " characters long");
        }
        if (registryUrl.length() < minlength) {
            LOGGER.warn("<registryUrl> is less then " + minlength + " characters long");
        }
        if (bootstrapServer.indexOf(":") == -1) {
            LOGGER.warn("<bootstrapServer> has no colon");
        }
        if (registryUrl.indexOf(":") == -1) {
            LOGGER.warn("<registryUrl> has no colon");
        }
    }
}
