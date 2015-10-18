namespace java org.webcache.thrift

struct SiteInfo {
    1: i32 refreshInterval;
    2: i32 maxDepth;
    3: i32 pageLimit;
    4: i32 timeout;
}

service Crawler {
    string getUrlToCrawl();
    void createSite(1: string domain, 2: SiteInfo info);
}