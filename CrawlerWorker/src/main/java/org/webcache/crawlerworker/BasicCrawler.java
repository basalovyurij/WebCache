package org.webcache.crawlerworker;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.parser.HtmlParseData;
import edu.uci.ics.crawler4j.url.WebURL;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.webcache.core.DataProvider;
import org.webcache.core.Helpers;
import org.webcache.core.MongoDataProvider;

public class BasicCrawler extends WebCrawler {
    
    private final static Logger logger = LogManager.getLogger(BasicCrawler.class);
    
    private final static Pattern FILTERS = Pattern.compile(".*(\\.(css|js|gif|jpg"
            + "|png|mp3|mp3|zip|gz))$");

    /**
     * This method receives two parameters. The first parameter is the page in
     * which we have discovered this new url and the second parameter is the new
     * url. You should implement this function to specify whether the given url
     * should be crawled or not (based on your crawling logic). In this example,
     * we are instructing the crawler to ignore urls that have css, js, git, ...
     * extensions . In this case, we didn't need the referringPage parameter to
     * make the decision.
     *
     * @param referringPage
     * @param url
     * @return
     */
    @Override
    public boolean shouldVisit(Page referringPage, WebURL url) {
        return !FILTERS.matcher(url.getURL().toLowerCase()).matches();
    }
    
    private final DataProvider provider = new MongoDataProvider();

    /**
     * This function is called when a page is fetched and ready to be processed
     * by your program.
     *
     * @param page
     */
    @Override
    public void visit(Page page) {
        String url = page.getWebURL().getURL();
        logger.info("URL: " + url);
        
        if (page.getParseData() instanceof HtmlParseData) {
            HtmlParseData htmlParseData = (HtmlParseData) page.getParseData();
            String text = htmlParseData.getText();
            String html = htmlParseData.getHtml();
            Set<WebURL> links = htmlParseData.getOutgoingUrls();
            
            logger.info("Text length: " + text.length());
            logger.info("Html length: " + html.length());
            logger.info("Number of outgoing links: " + links.size());
            
            List<String> urls = links.stream().map(l -> l.getURL()).collect(Collectors.toList());
            
            try {
                provider.addPages(Helpers.getDomain(url), urls);
                provider.setPageText(url, text);
            } catch (Exception e) {
                logger.error("", e);
            }
        }
    }
}
