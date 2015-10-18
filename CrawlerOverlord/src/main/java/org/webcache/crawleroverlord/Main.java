package org.webcache.crawleroverlord;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.webcache.thrift.Crawler;

/**
 *
 * @author yurij
 */
public class Main {

    private final static Logger logger = LogManager.getLogger(Main.class);
    
    public static void main(String[] args) {
        try {
            CrawlerService handler = new CrawlerService();
            Crawler.Processor processor = new Crawler.Processor(handler);

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
