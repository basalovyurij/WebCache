package org.webcache.crawleroverlord;

import java.net.URISyntaxException;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.time.DateUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.webcache.core.DataProvider;
import org.webcache.core.Helpers;
import org.webcache.core.MongoDataProvider;
import org.webcache.thrift.Crawler;
import org.webcache.thrift.SiteInfo;

/**
 *
 * @author yurij
 */
public class CrawlerService implements Crawler.Iface {
    
    private final static Logger logger = LogManager.getLogger(CrawlerService.class);
    
    private final static int CACHE_TIMEOUT = 300 * 1000;
    private final static int BATCH_SIZE = 10000;
    
    private final DataProvider provider = new MongoDataProvider();
    
    private Map<String, SiteInfo> cachedSites;
    private int pagesCount;
    private long lastLoadCacheTime = 0;
    private final List<String> cachedUrls = new LinkedList<>();
    
    @Override
    public String getUrlToCrawl() throws TException {
        if (tryLoadNewPages()) {
            return cachedUrls.remove(0);
        } else {
            return "";
        }
    }
    
    private boolean tryLoadNewPages() {
        if (cachedUrls.isEmpty()) {
            refreshCacheIfNeeds();
            
            int skip = 0;
            while (skip < pagesCount) {
                Map<String, Date> pages = provider.getPagesLastCrawlDate(skip, BATCH_SIZE);
                
                pages.entrySet().stream().forEach(e -> {
                    if (needCrawlUrl(e.getKey(), e.getValue())) {
                        cachedUrls.add(e.getKey());
                    }
                });
                
                skip += BATCH_SIZE;
            }
        }
        
        return cachedUrls.isEmpty();
    }
    
    private void refreshCacheIfNeeds() {
        if (lastLoadCacheTime + CACHE_TIMEOUT < new Date().getTime()) {
            cachedSites = provider.getSites();
            pagesCount = provider.getPageCount();
            lastLoadCacheTime = new Date().getTime();
        }
    }
    
    private boolean needCrawlUrl(String url, Date lastCrawlDate) {
        try {
            int refreshInterval = cachedSites.get(Helpers.getDomain(url)).getRefreshInterval();
            return DateUtils.addDays(lastCrawlDate, refreshInterval).before(new Date());
        } catch (URISyntaxException e) {
            logger.warn("", e);
            return false;
        }
    }
    
    @Override
    public void createSite(String domain, SiteInfo info) throws TException {
        provider.createSite(domain, info);
    }
}
