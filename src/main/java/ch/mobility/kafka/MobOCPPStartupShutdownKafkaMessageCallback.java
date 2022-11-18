package ch.mobility.kafka;

import ch.mobility.charger2mob.MobOCPPShutdownNotification;
import ch.mobility.charger2mob.MobOCPPStartupNotification;

public interface MobOCPPStartupShutdownKafkaMessageCallback {
    void processMobOCPPStartupNotification(MobOCPPStartupNotification mobOCPPStartupNotification);
    void processMobOCPPShutdownNotification(MobOCPPShutdownNotification mobOCPPShutdownNotification);
}
