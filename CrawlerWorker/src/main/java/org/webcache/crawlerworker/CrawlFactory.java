package org.webcache.crawlerworker;

import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.CrawlController;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtConfig;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.webcache.core.DataProvider;
import org.webcache.core.MongoDataProvider;
import org.webcache.thrift.CrawlState;
import org.webcache.thrift.SiteInfo;

/**
 *
 * @author yurij
 */
public class CrawlFactory {

    private static final String CRAWL_STORAGE_FOLDER = ".";
    private static final int NUMBER_OF_THREADS = 50;

    private static final DataProvider provider = new MongoDataProvider();

    private final Map<String, CrawlController> controllers = new HashMap<>();

    public void start(String domain, SiteInfo info) throws Exception {
        CrawlConfig crawlConfig = new CrawlConfig();
        crawlConfig.setCrawlStorageFolder(CRAWL_STORAGE_FOLDER);
        crawlConfig.setMaxPagesToFetch(info.getPageLimit());
        crawlConfig.setMaxDepthOfCrawling(info.getMaxDepth());
        crawlConfig.setPolitenessDelay(info.getTimeout());
        crawlConfig.setResumableCrawling(true);

        /*
         * Instantiate the controller for this crawl.
         */
        PageFetcher pageFetcher = new PageFetcher(crawlConfig);
        RobotstxtConfig robotstxtConfig = new RobotstxtConfig();
        RobotstxtServer robotstxtServer = new RobotstxtServer(robotstxtConfig, pageFetcher);
        CrawlController controller = new CrawlController(crawlConfig, pageFetcher, robotstxtServer);

        /*
         * For each crawl, you need to add some seed urls. These are the first
         * URLs that are fetched and then the crawler starts following links
         * which are found in these pages
         */
        controller.addSeed("http://" + domain + "/");

        /*
         * Start the crawl. This is a blocking operation, meaning that your code
         * will reach the line after this only when crawling is finished.
         */
        controller.startNonBlocking(BasicCrawler.class, NUMBER_OF_THREADS);
        setStartCrawl(domain);
        controllers.put(domain, controller);
    }

    public boolean isWorking() {
        Set<String> keys = new HashSet<>(controllers.keySet());

        keys.stream()
                .filter((key) -> (controllers.get(key).isFinished()))
                .forEach((key) -> {
                    setEndCrawl(key);
                    controllers.remove(key);
                });

        return !controllers.isEmpty();
    }

    public void stop(String domain) {
        controllers.get(domain).shutdown();
        setEndCrawl(domain);
        controllers.remove(domain);
    }

    private void setStartCrawl(String domain) {
        SiteInfo site = provider.getSite(domain);

        site.setCrawlState(CrawlState.CRAWLING);
        site.setNextCrawlDateL(site.getNextCrawlDateL()
                + site.getRefreshInterval() * TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS));

        provider.createOrReplaceSite(domain, site);
    }

    private void setEndCrawl(String domain) {
        SiteInfo site = provider.getSite(domain);

        site.setCrawlState(CrawlState.OK);

        provider.createOrReplaceSite(domain, site);
    }
}
