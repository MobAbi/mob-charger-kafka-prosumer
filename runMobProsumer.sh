#!/bin/bash
java -cp target/mob-charger-kafka-prosumer-0.0.1-jar-with-dependencies.jar ch.mobility.kafka.MobProsumer $1 $2
#java -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=1234 -cp target/mob-charger-kafka-prosumer-0.0.1-jar-with-dependencies.jar ch.mobility.kafka.MobProsumer $1 $2
