package ch.mobility.kafka;

import ch.mobility.charger2mob.*;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Console;
import java.lang.reflect.Field;
import java.util.List;
import java.util.UUID;

public class MobProsumer {

    private static Logger LOGGER = LoggerFactory.getLogger(MobProsumer.class);

    private static final long MAX_WAIT_FOR_RESPONSE_MS = 5000L;
    private static final int MAX_NUMBER_OF_OCPP_BACKENDS = 5;

    private static final String RESPONSE_INFO = "ResponseInfo";

    public static void main(String[] args) throws Exception {

        // Check arguments length value
        if (args.length != 2){
            LOGGER.info("MobProsumer <bootstrapServer> <registryUrl>");
            return;
        }

        final String bootstrapServer = args[0];
        final String registryUrl = args[1];

        validate(bootstrapServer, registryUrl);

        AvroProducer.init(bootstrapServer, registryUrl, TopicnameHelper.MOB2CHARGER);
        IncomingKafkaMessageCallback4ExpectedClass callback = new IncomingKafkaMessageCallback4ExpectedClass();
        AvroConsumer.init(bootstrapServer, registryUrl, TopicnameHelper.CHARGER2MOB, "MobProsumer", callback);

        try {
            Console console = System.console();
            boolean run = true;

            while (run) {
                System.out.print("\n> ");
                String input = console.readLine();
                if (input != null) {
                    List<? extends GenericRecord> received = null;

                    String messageId = UUID.randomUUID().toString();
                    String[] splitted = input.trim().split(" ");
                    //LOGGER.info("splitted length=" + splitted.length + ", Values: " + Arrays.stream(splitted).collect(Collectors.toList()));

                    if (match(splitted, 1, "quit")) {
                        log("Bye");
                        run = false;
                    } else if (match(splitted, 1,"help")) {
                        printHelp();
                    } else if (match(splitted, 1, "status")) {
                        AvroProducer.get().requestStatusConnected(messageId);
//                        received = callback.receive(CSStatusConnectedResponse.class, messageId, MAX_WAIT_FOR_RESPONSE_MS, MAX_NUMBER_OF_OCPP_BACKENDS);
                        received = callback.receive(CSStatusConnectedResponse.class, messageId, MAX_WAIT_FOR_RESPONSE_MS, MAX_NUMBER_OF_OCPP_BACKENDS);
                    } else if (match(splitted, 1, 2, "recently")) {
                        Integer daysOfHistoryData = splitted.length > 1 ? Integer.valueOf(splitted[1]) : null;
                        AvroProducer.get().requestRecentlyConnected(messageId, daysOfHistoryData);
                        received = callback.receive(CSRecentlyConnectedResponse.class, messageId, MAX_WAIT_FOR_RESPONSE_MS, MAX_NUMBER_OF_OCPP_BACKENDS);
                    } else if (match(splitted, 2, 4, "status")) {
                        String id = splitted[1];
                        Integer connectorId = splitted.length > 2 ? Integer.valueOf(splitted[2]) : null;
                        Integer daysOfHistoryData = splitted.length > 3 ? Integer.valueOf(splitted[3]) : null;
                        AvroProducer.get().requestStatusForId(messageId, id, connectorId, daysOfHistoryData);
                        received = callback.receive(CSStatusForIdResponse.class, messageId, MAX_WAIT_FOR_RESPONSE_MS, 1);
                    } else if (match(splitted, 3, 5, "start")) {
                        String id = splitted[1];
                        String tagId = splitted[2];
                        Integer connectorId = Integer.valueOf(splitted.length > 3 ? splitted[3] : "1");
                        Integer maxCurrent = (splitted.length > 4 ? Integer.valueOf(splitted[4]) : null);
                        Integer numberOfPhases = (splitted.length > 5 ? Integer.valueOf(splitted[5]) : null);
                        AvroProducer.get().requestStart(messageId, id, connectorId, tagId, maxCurrent, numberOfPhases);
                        received = callback.receive(CSStartChargingResponse.class, messageId, MAX_WAIT_FOR_RESPONSE_MS, 1);
                    } else if (match(splitted, 2, 3, "stop")) {
                        String id = splitted[1];
                        Integer connectorId = Integer.valueOf(splitted.length > 2 ? splitted[2] : "1");
                        AvroProducer.get().requestStop(messageId, id, connectorId);
                        received = callback.receive(CSStopChargingResponse.class, messageId, MAX_WAIT_FOR_RESPONSE_MS, 1);
                    } else if (match(splitted, 2, "reset")) {
                        String id = splitted[1];
                        AvroProducer.get().requestReset(messageId, id);
                        received = callback.receive(CSChangeChargingCurrentResponse.class, messageId, MAX_WAIT_FOR_RESPONSE_MS, 1);
                    } else if (match(splitted, 2, 3,"unlock")) {
                        String id = splitted[1];
                        Integer connectorId = Integer.valueOf(splitted.length > 2 ? splitted[2] : "1");
                        AvroProducer.get().requestUnlock(messageId, id, connectorId);
                        received = callback.receive(CSUnlockResponse.class, messageId, MAX_WAIT_FOR_RESPONSE_MS, 1);
                    } else if (match(splitted, 3, 5, "profile")) {
                        String id = splitted[1];
                        Integer maxCurrent = Integer.valueOf(splitted[2]);
                        Integer connectorId = Integer.valueOf(splitted.length > 3 ? splitted[3] : "1");
                        Integer numberOfPhases = (splitted.length > 4 ? Integer.valueOf(splitted[4]) : null);
                        AvroProducer.get().requestProfile(messageId, id, connectorId, maxCurrent, numberOfPhases);
                        received = callback.receive(CSChangeChargingCurrentResponse.class, messageId, MAX_WAIT_FOR_RESPONSE_MS, 1);
                    } else if (match(splitted, 3, 4, "trigger")) {
                        String id = splitted[1];
                        String trigger = splitted[2];
                        Integer connectorId = Integer.valueOf(splitted.length > 3 ? splitted[3] : "1");
                        AvroProducer.get().requestTrigger(messageId, id, connectorId, trigger);
                        received = callback.receive(CSTriggerResponse.class, messageId, MAX_WAIT_FOR_RESPONSE_MS, 1);
                    } else if (match(splitted, 3, 4, "availability")) {
                        String id = splitted[1];
                        boolean isOperative = Boolean.valueOf(splitted[2]).booleanValue();
                        Integer connectorId = Integer.valueOf(splitted.length > 3 ? splitted[3] : "1");
                        AvroProducer.get().requestChangeOperationalStatus(messageId, id, connectorId, isOperative);
                        received = callback.receive(CSOperationalStatusResponse.class, messageId, MAX_WAIT_FOR_RESPONSE_MS, 1);
                    }
                    else {
                        log("Unknown request: " + input);
                        printHelp();
                    }

                    handleReceived(received, messageId);
                }
            }
        } finally {
            AvroProducer.get().close();
            AvroConsumer.get().close();
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

    private static boolean hasMessageId(GenericRecord value, String expectedMessageId) {
        if (value == null) {
            throw new IllegalArgumentException("Parameter <value> must not be null");
        }
        if (expectedMessageId == null) {
            throw new IllegalArgumentException("Parameter <expectedMessageId> must not be null");
        }
        if (value instanceof MobOCPPStartupNotification || value instanceof MobOCPPShutdownNotification) {
            return true;
        }
        try {
            final Field responseInfoField = value.getClass().getDeclaredField(RESPONSE_INFO);
            responseInfoField.setAccessible(true);
            final CSResponse csResponse = (CSResponse)responseInfoField.get(value);
            if (expectedMessageId.equals(csResponse.getMessageId())) {
                return true;
            }
        } catch (NoSuchFieldException e ) {
            LOGGER.error("Missing field <" + RESPONSE_INFO + "> in Class <" + value.getClass().getName() + ">");
            e.printStackTrace(System.err);
//                throw new IllegalStateException("Missing field <" + RESPONSE_INFO + "> in Class <" + value.getClass().getName() + ">", e);
        } catch (IllegalAccessException e) {
            LOGGER.error("IllegalAccessException: " + e.getMessage());
            e.printStackTrace(System.err);
//                throw new IllegalStateException(e.getMessage(), e);
        }
        return false;
    }

    private static void handleReceived(List<? extends GenericRecord> received, String expectedMessageId) {
        if (received != null) {
            try {
                for (GenericRecord record : received) {
                    if (hasMessageId(record, expectedMessageId)) {
                        if (record instanceof CSResponse) {
                            CSResponse response = (CSResponse) record;
                            LOGGER.info("CSResponse: " + response);
                        } else if (record instanceof CSStatusForIdResponse) {
                            CSStatusForIdResponse csStatusForIdResponse = (CSStatusForIdResponse)record;
                            LOGGER.info("CSStatusForIdResponse: " +
                                    csStatusForIdResponse.getResponseInfo() + ", CS " +
                                    csStatusForIdResponse.getStatus().getId() + ", Backend-Status " +
                                    csStatusForIdResponse.getStatus().getBackendStatus() + ", Last-Contact " +
                                    csStatusForIdResponse.getStatus().getLastContact());
                            for (CPStatus cpStatus : csStatusForIdResponse.getStatus().getCPStatusList()) {
                                if (cpStatus.getCPStatusHistoryList().isEmpty() && cpStatus.getCPTransactionHistoryList().isEmpty()) {
                                    LOGGER.info(" " + cpStatus);
                                } else {
                                    LOGGER.info(" CPStatus: ConnectorId " + cpStatus.getConnectorId() +
                                            ", ConnectorStatus " + cpStatus.getConnectorStatus() +
                                            ", ChargingState " + cpStatus.getChargingState());
                                    for (CPStatusHistoryEntry statusHistory : cpStatus.getCPStatusHistoryList()) {
                                        LOGGER.info("  CPStatusHistoryEntry: " + statusHistory);
                                    }
                                    for (CPTransactionHistoryEntry txHistory : cpStatus.getCPTransactionHistoryList()) {
                                        LOGGER.info("  CPTransactionHistoryEntry: " + txHistory);
                                    }
                                }
                            }
                        } else if (record instanceof CSStatusConnectedResponse) {
                            CSStatusConnectedResponse csStatusConnectedResponse = (CSStatusConnectedResponse)record;
                            if (csStatusConnectedResponse.getResponseInfo().getError() != null) {
                                LOGGER.error("CSStatusConnectedResponse: " + csStatusConnectedResponse.getResponseInfo().getError());
                            } else {
                                LOGGER.info("\nCSStatusConnectedResponse " +
                                        csStatusConnectedResponse.getResponseInfo().getOCPPBackendId() +
                                        " " + csStatusConnectedResponse.getResponseInfo().getResponseCreatedAt() +
                                        ": " + csStatusConnectedResponse.getCSStatusList().size());
                                for (CSStatusConnected csStatusConnected : csStatusConnectedResponse.getCSStatusList()) {
                                    LOGGER.info(" " + csStatusConnected);
                                }
                            }
                        } else if (record instanceof CSRecentlyConnectedResponse) {
                            CSRecentlyConnectedResponse csRecentlyConnectedResponse = (CSRecentlyConnectedResponse)record;
                            if (csRecentlyConnectedResponse.getResponseInfo().getError() != null) {
                                LOGGER.error("CSRecentlyConnectedResponse: " + csRecentlyConnectedResponse.getResponseInfo().getError());
                            } else {
                                LOGGER.info("\nCSRecentlyConnectedResponse " +
                                        csRecentlyConnectedResponse.getResponseInfo().getOCPPBackendId() +
                                        " " + csRecentlyConnectedResponse.getResponseInfo().getResponseCreatedAt() +
                                        ": " + csRecentlyConnectedResponse.getCSRecentlyList().size());
                                for (CSRecentlyConnected csRecentlyConnected : csRecentlyConnectedResponse.getCSRecentlyList()) {
                                    LOGGER.info(" " + csRecentlyConnected);
                                }
                            }
                        } else {
                            //                            System.out.printf("\n%s: offset = %d, key = %s, value = %s\n",
                            //                                    record.value().getClass().getSimpleName(), record.offset(), record.key(), record.value());
                            System.out.printf("\n%s: %s\n", record.getClass().getSimpleName(), record);
                        }
//                        System.out.print("\n> ");
                    } else {
                        //out.println("Ignored Message: " + record.value());
                    }
                }
            } catch (WakeupException e) {
                // Nichts machen!
            } catch (Exception e) {
                LOGGER.error("Exception: " + e.getMessage());
                if (e.getCause() == null) {
                    e.printStackTrace(System.err);
                } else {
                    LOGGER.error("Cause: " + e.getCause().getMessage());
                }
            }
        }
    }

    private static void printHelp() {
        String help = "Usage:";
        help += "\n status";
        help += "\n recently [<daysOfHistoryData>]";
        help += "\n status <csId> [<connectorId> [<daysOfHistoryData>]]";
        help += "\n start <csId> <tagId> [<connectorId> [<maxCurrent 0-160> [numberOfPhases 1-3]]]";
        help += "\n stop <csId> [<connectorId>]";
        help += "\n reset <csId>";
        help += "\n unlock <csId> [<connectorId>]";
        help += "\n profile <csId> <maxCurrent 0-160> [<connectorId> [numberOfPhases 1-3]]";
        help += "\n trigger <csId> <HEARTBEAT|METER|STATUS|BOOT|DIAGNOSTICS|FIRMWARE> [<connectorId>]";
        help += "\n availability <csId> <true|false> [<connectorId>]";
        LOGGER.info(help);
    }

    private static boolean match(String[] inputArray, int minLength, String...values) {
        return match(inputArray, minLength, minLength, values);
    }

    private static boolean match(String[] inputArray, int minLength, int maxLength, String...values) {
        if (inputArray.length >= minLength && inputArray.length <= maxLength) {
            for (int i = 0; i < values.length; i++) {
                if (inputArray[i].equalsIgnoreCase(values[i])) {
//                    log("match true: " + Arrays.toString(inputArray));
                    return true;
                }
            }
        }
//        log("match false: " + Arrays.toString(inputArray));
        return false;
    }

    private static void log(String message) {
        LOGGER.info(message);
    }
}
