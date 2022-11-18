package ch.mobility.kafka;

import ch.mobility.charger2mob.*;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IncomingKafkaMessageCallback4ExpectedClass<T extends GenericRecord> implements IncomingKafkaMessageCallback {

    private static Logger LOGGER = LoggerFactory.getLogger(IncomingKafkaMessageCallback4ExpectedClass.class);

    private static final String RESPONSE_INFO = "ResponseInfo";
    private static final String BACKEND_STATUS = "BackendStatus";

    private Class expectedClass = null;
    private String expectedMessageId = null;
    private List<T> receivedMessages = null;
    private Map<String, Integer>statistics = null;

    public void receive(Class expectedClass, String expectedMessageId) {
        LOGGER.trace("Start with expectedClass=" + expectedClass + " and expectedMessageId=" + expectedMessageId);
        this.expectedClass = expectedClass;
        this.expectedMessageId = expectedMessageId;
        this.receivedMessages = new ArrayList<>();
        this.statistics = new HashMap<>();
    }

    public List<T> getReceived() {
        return this.receivedMessages;
    }

    @Override
    public void processIncomingKafkaMessage(GenericRecord message) {
        if (this.expectedClass != null) {
            // Startup- und Shutdown Notifizierungen nicht verarbeiten
            if (message.getClass().isAssignableFrom(MobOCPPStartupNotification.class) ||
                message.getClass().isAssignableFrom(MobOCPPShutdownNotification.class)) {
                throw new IllegalStateException("Das sollte im AvroConsumer abgehandelt werden !!!");
            } else {
                if (message.getClass().isAssignableFrom(this.expectedClass)) {
                    final T value = (T) message;
                    if (hasMessageId(value, this.expectedMessageId)) {
                        if (isConnected(value)) {
                            this.receivedMessages.add(value);
                            LOGGER.trace("Match: " + this.expectedClass);
                        } else {
                            LOGGER.trace("Nicht verbunden " + message);
                            increaseStatisticsCounter("Nicht verbunden");
                        }
                    } else {
                        LOGGER.trace("MessageId passt nicht: " + this.expectedMessageId + " <> " + message);
                        increaseStatisticsCounter("MessageId passt nicht");
                    }
                } else {
                    LOGGER.trace("Kein Match: " + this.expectedClass + " <> " + message);
                    increaseStatisticsCounter("Kein Match mit expected");
                }
            }
        } else {
            LOGGER.trace("Kein expected: " + message);
            increaseStatisticsCounter("Kein expected");
        }
    }

    private void increaseStatisticsCounter(String type) {
        if (statistics != null) {
            Integer counter = statistics.get(type);
            if (counter == null) {
                counter = Integer.valueOf(0);
            }
            counter = Integer.valueOf(counter.intValue() + 1);
            statistics.put(type, counter);
        }
    }

    private boolean hasMessageId(T value, String expectedMessageId) {
        if (value == null) {
            throw new IllegalArgumentException("Parameter <value> must not be null");
        }
        if (expectedMessageId == null) {
            throw new IllegalArgumentException("Parameter <expectedMessageId> must not be null");
        }
        try {
            final Field responseInfoField = value.getClass().getDeclaredField(RESPONSE_INFO);
            responseInfoField.setAccessible(true);
            final CSResponse csResponse = (CSResponse)responseInfoField.get(value);
            if (expectedMessageId.equals(csResponse.getMessageId())) {
                return true;
            }
        } catch (NoSuchFieldException e ) {
            throw new IllegalStateException("Missing field <" + RESPONSE_INFO + "> in Class <" + value.getClass().getName() + ">", e);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
        return false;
    }

    private boolean isConnected(T value) {
        final CSBackendStatusEnum csBackendStatusEnum;
        if (value.getClass().isAssignableFrom(CSStatusConnectedResponse.class)) {
            csBackendStatusEnum = CSBackendStatusEnum.CS_CONNECTED;
        } else if (value.getClass().isAssignableFrom(CSRecentlyConnectedResponse.class)) {
            // Little trick here ....not so nice....
            csBackendStatusEnum = CSBackendStatusEnum.CS_CONNECTED;
        } else if (value.getClass().isAssignableFrom(CSStatusForIdResponse.class)) {
            CSStatusForIdResponse csStatusForIdResponse = (CSStatusForIdResponse)value;
            csBackendStatusEnum = csStatusForIdResponse.getStatus().getBackendStatus();
        } else {
            try {
                final Field field = value.getClass().getDeclaredField(BACKEND_STATUS);
                field.setAccessible(true);
                csBackendStatusEnum = (CSBackendStatusEnum)field.get(value);
            } catch (NoSuchFieldException e ) {
                throw new IllegalStateException("Missing field <" + BACKEND_STATUS + "> in Class <" + value.getClass().getName() + ">", e);
            } catch (IllegalAccessException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        }

        return CSBackendStatusEnum.CS_CONNECTED.equals(csBackendStatusEnum);
    }

    public List<T> receive(Class expectedClass, String expectedMessageId, long wait, int maxMobOCPPBackends) {
        checkParameter(expectedClass, expectedMessageId, wait, maxMobOCPPBackends);
        receive(expectedClass, expectedMessageId);
        LOGGER.debug("Start receive, waiting " + wait + " ms...");
        final long start = System.currentTimeMillis();
        boolean doWait = true;
        while (doWait) {
            try { Thread.sleep(50); } catch (InterruptedException e) {}
            long waited = System.currentTimeMillis() - start;
            if (waited > wait) {
                LOGGER.debug("Timeout: " + wait);
                doWait = false;
            }
            //if (consumerThread.getReceived().size() >= maxMobOCPPBackends) {
            if (getReceived().size() >= maxMobOCPPBackends) {
                LOGGER.debug("Expected number of responses received: " + maxMobOCPPBackends);
                doWait = false;
            }
        }
        List<T> received = getReceived();
        LOGGER.debug("Statistics: " + statistics);
        receive(null, null);
        LOGGER.info("Empfangen: " + received);
        return received;
    }

    private void checkParameter(Class expectedClass, String expectedMessageId, long wait, int maxMobOCPPBackends) {
        if (expectedClass == null) {
            throw new IllegalArgumentException("Parameter <expectedClass> darf nicht <null> sein");
        }
        if (expectedMessageId == null) {
            throw new IllegalArgumentException("Parameter <expectedMessageId> darf nicht <null> sein");
        }
        if (wait <= 0) {
            throw new IllegalArgumentException("Parameter <wait> muss groesser 0 sein");
        }
        if (maxMobOCPPBackends <= 0) {
            throw new IllegalArgumentException("Parameter <maxMobOCPPBackends> muss groesser 0 sein");
        }
    }
}
