#!/bin/bash
java -cp target/mobocpp-prosumer-1.1.5-jar-with-dependencies.jar ch.mobility.kafka.MobProsumer $1 $2
#java -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=1234 -cp target/mobocpp-prosumer-1.1.5-jar-with-dependencies.jar ch.mobility.kafka.MobProsumer $1 $2
