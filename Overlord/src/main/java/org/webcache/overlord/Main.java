package org.webcache.overlord;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.webcache.thrift.Overlord;

/**
 *
 * @author yurij
 */
public class Main {

    private final static Logger logger = LogManager.getLogger(Main.class);
    
    public static void main(String[] args) {
        try {
            OverlordService handler = new OverlordService();
            Overlord.Processor processor = new Overlord.Processor(handler);

            new Thread(() -> {
                try {
                    TServerTransport serverTransport = new TServerSocket(9090);
                    TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));
                    
                    logger.info("Starting the server...");
                    server.serve();
                } catch (TTransportException e) {
                    logger.fatal("", e);
                }
            }).start();
        } catch (Exception x) {
            logger.fatal("", x);
        }
    }
}
