package org.webcache.core;

import java.util.Date;
import java.util.List;
import java.util.Map;
import org.webcache.thrift.SiteInfo;

/**
 *
 * @author yurij
 */
public interface DataProvider {
    Map<String, SiteInfo> getSites();
    SiteInfo getSite(String domain);
    void createSite(String domain, SiteInfo info);
    List<String> getPages(String domain);
    int getPageCount();
    Map<String, Date> getPagesLastCrawlDate(int skip, int limit);
    PageInfo getPageInfo(String url);
    void createPage(String url, PageInfo info);
    String getPageText(String url);
    void setPageText(String url, String text);
    void setPageState(String url, PageCrawlState state);
    void setPageError(String url, String error);
}
