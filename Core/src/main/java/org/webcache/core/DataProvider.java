package org.webcache.core;

import java.util.List;
import java.util.Map;
import org.webcache.thrift.SiteInfo;

/**
 *
 * @author yurij
 */
public interface DataProvider {
    Map<String, SiteInfo> getSites();
    List<String> getSitesToCrawl();
    SiteInfo getSite(String domain);
    void createOrReplaceSite(String domain, SiteInfo info);
    List<String> getPages(String domain);
    void addPages(String domain, List<String> pages);
    String getPageText(String url);
    void setPageText(String url, String text);;
}
