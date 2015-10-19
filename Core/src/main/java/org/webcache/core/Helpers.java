package org.webcache.core;

import java.net.URI;
import java.net.URISyntaxException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 *
 * @author yurij
 */
public class Helpers {

    private final static Logger logger = LogManager.getLogger(Helpers.class);

    public static String getDomain(String url) {
        try {
            URI uri = new URI(url);
            String domain = uri.getHost();
            return domain.startsWith("www.") ? domain.substring(4) : domain;
        } catch (URISyntaxException e) {
            logger.warn("", e);            
            return null;
        }
    }

    public static String getRobotsTxtUrl(String domain) {
        return "http://" + domain + "/robots.txt";
    }

    public static boolean isRobotsTxtUrl(String url) {
        return url.endsWith("/robots.txt");
    }
}
