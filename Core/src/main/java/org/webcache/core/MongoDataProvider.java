package org.webcache.core;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOptions;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.bson.Document;
import org.webcache.thrift.CrawlState;
import org.webcache.thrift.SiteInfo;

/**
 *
 * @author yurij
 */
public class MongoDataProvider implements DataProvider {

    private final MongoCollection sitesInfo;
    private final MongoCollection sitesPages;
    private final MongoCollection pagesTexts;

    public MongoDataProvider() {
        MongoClient client = new MongoClient("localhost");
        MongoDatabase database = client.getDatabase("WebCache");

        sitesInfo = database.getCollection("sites_info");
        sitesPages = database.getCollection("sites_pages");
        pagesTexts = database.getCollection("pages_texts");
    }

    @Override
    public Map<String, SiteInfo> getSites() {
        Map<String, SiteInfo> res = new HashMap<>();

        try (MongoCursor<Document> cursor = sitesInfo.find().iterator()) {
            while (cursor.hasNext()) {
                Document doc = cursor.next();

                res.put(doc.getString("domain"), extractSite(doc));
            }
        }

        return res;
    }

    @Override
    public List<String> getSitesToCrawl() {
        List<String> res = new LinkedList<>();

        try (MongoCursor<Document> cursor = sitesInfo.find(
                Filters.and(
                        Filters.eq("crawlState", CrawlState.OK.getValue()),
                        Filters.lt("nextCrawlDate", new Date())))
                .projection(Projections.include("domain"))
                .iterator()) {
            while (cursor.hasNext()) {
                Document doc = cursor.next();
                res.add(doc.getString("domain"));
            }
        }

        return res;
    }

    @Override
    public SiteInfo getSite(String domain) {
        try (MongoCursor<Document> cursor = sitesInfo.find(Filters.eq("domain", domain)).iterator()) {
            while (cursor.hasNext()) {
                return extractSite(cursor.next());
            }
        }

        return null;
    }

    private SiteInfo extractSite(Document doc) {
        SiteInfo info = new SiteInfo();
        if (doc.containsKey("refreshInterval")) {
            info.setRefreshInterval(doc.getInteger("refreshInterval"));
        }
        if (doc.containsKey("maxDepth")) {
            info.setMaxDepth(doc.getInteger("maxDepth"));
        }
        if (doc.containsKey("pageLimit")) {
            info.setPageLimit(doc.getInteger("pageLimit"));
        }
        if (doc.containsKey("timeout")) {
            info.setTimeout(doc.getInteger("timeout"));
        }
        if (doc.containsKey("nextCrawlDate")) {
            info.setNextCrawlDateL(doc.getDate("nextCrawlDate").getTime());
        }
        if (doc.containsKey("crawlState")) {
            info.setCrawlState(CrawlState.findByValue(doc.getInteger("crawlState")));
        }
        
        return info;
    }

    @Override
    public void createOrReplaceSite(String domain, SiteInfo info) {
        Document doc = new Document("domain", domain)
                .append("nextCrawlDate", new Date(info.getNextCrawlDateL()))
                .append("refreshInterval", info.getRefreshInterval())
                .append("maxDepth", info.getMaxDepth())
                .append("pageLimit", info.getPageLimit())
                .append("timeout", info.getTimeout())
                .append("crawlState", CrawlState.OK.getValue());

        sitesInfo.replaceOne(
                new Document("domain", domain),
                doc,
                new UpdateOptions().upsert(true));

        doc = new Document("domain", domain)
                .append("pages", new LinkedList<>());

        sitesPages.replaceOne(
                new Document("domain", domain),
                doc,
                new UpdateOptions().upsert(true));
    }

    @Override
    public List<String> getPages(String domain) {
        try (MongoCursor<Document> cursor = sitesPages.find(Filters.eq("domain", domain)).iterator()) {
            if (cursor.hasNext()) {
                Document doc = cursor.next();
                return doc.get("pages", List.class);
            }
        }

        return new LinkedList<>();
    }

    @Override
    public void addPages(String domain, List<String> pages) {
        pages.addAll(getPages(domain));
        List<String> newPages = pages.stream().distinct().collect(Collectors.toList());

        sitesPages.replaceOne(
                Filters.eq("domain", domain),
                new Document("domain", domain).append("pages", newPages),
                new UpdateOptions().upsert(true));
    }

    @Override
    public String getPageText(String url) {
        try (MongoCursor<Document> cursor = pagesTexts
                .find(Filters.eq("url", url))
                .iterator()) {
            if (cursor.hasNext()) {
                Document doc = cursor.next();
                return doc.getString("text");
            }
        }

        return null;
    }

    @Override
    public void setPageText(String url, String text) {
        pagesTexts.replaceOne(
                Filters.eq("url", url),
                new Document("url", url).append("text", text),
                new UpdateOptions().upsert(true));
    }
}
