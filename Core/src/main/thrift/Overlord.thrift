namespace java org.webcache.thrift

enum CrawlState {
    OK = 1,
    CRAWLING = 2
}

struct SiteInfo {
    1: i32 refreshInterval;
    2: i32 maxDepth;
    3: i32 pageLimit;
    4: i32 timeout;
    5: i64 nextCrawlDateL; 
    6: CrawlState crawlState;
}

service Overlord {
    string getSiteToCrawl();
    void createSite(1: string domain, 2: SiteInfo info);
}