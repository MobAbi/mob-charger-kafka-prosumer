package ch.mobility.kafka;

import java.io.IOException;
import java.net.Socket;

public class KafkaHelper {

    static boolean isKafkaAvailable(String hostWithPort) {
        String[] split = hostWithPort.split(":");
        if (split.length != 2) {
            throw new IllegalArgumentException("Host:Port expected: " + hostWithPort);
        }
        String host = split[0];
        int port = Integer.valueOf(split[1]).intValue();
        return isKafkaAvailable(host, port);
    }

    static boolean isKafkaAvailable(String host, int port) {
        Socket s = null;
        try {
            s = new Socket(host, port);

            // If the code makes it this far without an exception it means
            // something is using the port and has responded.
//            System.out.println(host + ":" + port + " is not available, so Kafka seems to run....");
            return true;
        } catch (IOException e) {
//            System.out.println(host + ":" + port + " is available, so Kafka seems *not* to run....");
            return false;
        } finally {
            if( s != null){
                try {
                    s.close();
                } catch (IOException e) {
                    throw new RuntimeException("You should handle this error." , e);
                }
            }
        }
    }
}
