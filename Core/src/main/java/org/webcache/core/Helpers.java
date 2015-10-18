package org.webcache.core;

import java.net.URI;
import java.net.URISyntaxException;

/**
 *
 * @author yurij
 */
public class Helpers {

    public static String getDomain(String url) throws URISyntaxException {
        URI uri = new URI(url);
        String domain = uri.getHost();
        return domain.startsWith("www.") ? domain.substring(4) : domain;
    }
}
