package org.webcache.core;

import java.util.Date;

/**
 *
 * @author yurij
 */
public class PageInfo {
    private Date lastCrawlDate;
    private PageCrawlState state;
    private String error;

    public PageInfo() {
    }

    public PageInfo(Date lastCrawlDate, PageCrawlState state) {
        this.lastCrawlDate = lastCrawlDate;
        this.state = state;
    }

    public Date getLastCrawlDate() {
        return lastCrawlDate;
    }

    public void setLastCrawlDate(Date lastCrawlDate) {
        this.lastCrawlDate = lastCrawlDate;
    }

    public PageCrawlState getState() {
        return state;
    }

    public void setState(PageCrawlState state) {
        this.state = state;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }
}
