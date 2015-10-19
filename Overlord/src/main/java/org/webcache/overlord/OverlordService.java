package org.webcache.overlord;

import java.net.URISyntaxException;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang.time.DateUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.webcache.core.DataProvider;
import org.webcache.core.Helpers;
import org.webcache.core.MongoDataProvider;
import org.webcache.thrift.Overlord;
import org.webcache.thrift.SiteInfo;

/**
 *
 * @author yurij
 */
public class OverlordService implements Overlord.Iface {

    private final static Logger logger = LogManager.getLogger(OverlordService.class);

    private final static int CACHE_TIMEOUT = 3 * 1000;
    private final static int BATCH_SIZE = 10000;

    private final DataProvider provider = new MongoDataProvider();

    private List<String> cachedSites;
    private long lastLoadCacheTime = 0;

    @Override
    public String getSiteToCrawl() throws TException {
        try {
            if (tryLoadNewPages()) {
                return cachedSites.remove(0);
            } else {
                return "";
            }
        } catch (Exception e) {
            logger.error("", e);
            throw new TException(e);
        }
    }

    private boolean tryLoadNewPages() {
        if (cachedSites == null || cachedSites.isEmpty()) {
            refreshCacheIfNeeds();
        }

        return !cachedSites.isEmpty();
    }

    private void refreshCacheIfNeeds() {
        if (lastLoadCacheTime + CACHE_TIMEOUT < new Date().getTime()) {
            cachedSites = provider.getSitesToCrawl();
            lastLoadCacheTime = new Date().getTime();
        }
    }

    @Override
    public void createSite(String domain, SiteInfo info) throws TException {
        try {
            provider.createOrReplaceSite(domain, info);
        } catch (Exception e) {
            logger.error("", e);
            throw new TException(e);
        }
    }
}
