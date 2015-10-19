package org.webcache.crawlerworker;

import java.util.Date;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.webcache.core.DataProvider;
import org.webcache.core.MongoDataProvider;
import org.webcache.thrift.CrawlState;
import org.webcache.thrift.Overlord;
import org.webcache.thrift.SiteInfo;

/**
 *
 * @author yurij
 */
public class Main {

    private final static Logger logger = LogManager.getLogger(BasicCrawler.class);

    private static final int WAIT_TIMEOUT = 3 * 1000;

    private static final DataProvider provider = new MongoDataProvider();

    public static void main(String[] args) throws Exception {
        initSites();
        
        CrawlFactory factory = new CrawlFactory();
        
        while (true) {
            if(!factory.isWorking()) {
                String domain = getNextSite();
                if(domain != null && !domain.isEmpty()) {
                    factory.start(domain, provider.getSite(domain));
                }
            }
            
            Thread.sleep(WAIT_TIMEOUT);
        }
    }

    private static String getNextSite() {
        try (TTransport transport = new TSocket("localhost", 9090)) {
            transport.open();

            TProtocol protocol = new TBinaryProtocol(transport);
            Overlord.Client client = new Overlord.Client(protocol);

            return client.getSiteToCrawl();
        } catch (TException e) {
            logger.error(e);
            return null;
        }
    }
    
    private static void initSites() throws Exception {
        try (TTransport transport = new TSocket("localhost", 9090)) {
            transport.open();

            TProtocol protocol = new TBinaryProtocol(transport);
            Overlord.Client client = new Overlord.Client(protocol);

            client.createSite(
                    "ru.wikipedia.org", 
                    new SiteInfo(
                            30, 
                            2, 
                            100, 
                            10, 
                            new Date().getTime(), 
                            CrawlState.OK));
        }
    }
}
