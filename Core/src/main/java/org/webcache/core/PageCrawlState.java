package org.webcache.core;

/**
 *
 * @author yurij
 */
public enum PageCrawlState {
    UNKHOWN(0),
    OK(1),
    CRAWLING(2),
    ERROR(3);
    
    public static PageCrawlState parse(int val) {
        for(PageCrawlState state : PageCrawlState.values()) {
            if (state.getValue() == val) {
                return state;
            }
        }
        
        return UNKHOWN;
    }
    
    private final int value;

    private PageCrawlState(int value) {
        this.value = value;
    }
    
    public int getValue() {
        return value;
    } 
}
