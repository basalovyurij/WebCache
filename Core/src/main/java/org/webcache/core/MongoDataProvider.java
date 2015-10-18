package org.webcache.core;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOptions;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.time.DateUtils;
import org.bson.Document;
import org.webcache.thrift.SiteInfo;

/**
 *
 * @author yurij
 */
public class MongoDataProvider implements DataProvider {

    private final MongoCollection sitesInfo;
    private final MongoCollection pagesInfo;
    private final MongoCollection sitesPages;
    private final MongoCollection pagesTexts;

    public MongoDataProvider() {
        MongoClient client = new MongoClient("localhost");
        MongoDatabase database = client.getDatabase("WebCache");

        sitesInfo = database.getCollection("sites_info");
        pagesInfo = database.getCollection("pages_info");
        sitesPages = database.getCollection("sites_pages");
        pagesTexts = database.getCollection("pages_texts");
    }

    @Override
    public final Map<String, SiteInfo> getSites() {
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

        return info;
    }

    @Override
    public void createSite(String domain, SiteInfo info) {
        String robotsTxtUrl = "http://" + domain + "/robots.txt";

        Document doc = new Document("domain", domain)
                .append("refreshInterval", info.getRefreshInterval())
                .append("maxDepth", info.getMaxDepth())
                .append("pageLimit", info.getPageLimit())
                .append("timeout", info.getTimeout());

        sitesInfo.insertOne(doc);

        doc = new Document("domain", domain)
                .append("pages", Arrays.asList(robotsTxtUrl));

        sitesPages.insertOne(doc);

        createPage(robotsTxtUrl, new PageInfo(new Date(100, 0, 1), PageCrawlState.OK));
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
    public int getPageCount() {
        return (int) pagesInfo.count();
    }

    @Override
    public Map<String, Date> getPagesLastCrawlDate(int skip, int limit) {
        Map<String, Date> res = new HashMap<>();

        try (MongoCursor<Document> cursor = pagesInfo
                .find(Filters.eq("state", PageCrawlState.OK.getValue()))
                .skip(skip)
                .limit(limit)
                .projection(Projections.include("url", "lastCrawlDate"))
                .iterator()) {
            while (cursor.hasNext()) {
                Document doc = cursor.next();
                res.put(doc.getString("url"), doc.getDate("lastCrawlDate"));
            }
        }

        return res;
    }

    @Override
    public PageInfo getPageInfo(String url) {
        try (MongoCursor<Document> cursor = pagesInfo
                .find(Filters.eq("url", url))
                .iterator()) {
            if (cursor.hasNext()) {
                Document doc = cursor.next();

                PageInfo info = new PageInfo();
                info.setLastCrawlDate(doc.getDate("lastCrawlDate"));
                info.setState(PageCrawlState.parse(doc.getInteger("state")));
                if (doc.containsKey("error")) {
                    info.setError(doc.getString("error"));
                }

                return info;
            }
        }

        return null;
    }

    @Override
    public void createPage(String url, PageInfo info) {
        Document doc = new Document("url", url)
                .append("lastCrawlDate", info.getLastCrawlDate())
                .append("state", info.getState());

        pagesInfo.insertOne(doc);
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
        pagesInfo.updateOne(
                Filters.eq("url", url),
                new Document("state", PageCrawlState.OK).append("lastCrawlDate", new Date()),
                new UpdateOptions().upsert(true));

        pagesTexts.updateOne(
                Filters.eq("url", url),
                new Document("url", url).append("text", text),
                new UpdateOptions().upsert(true));
    }

    @Override
    public void setPageError(String url, String error) {
        pagesInfo.updateOne(
                Filters.eq("url", url),
                new Document("error", error),
                new UpdateOptions().upsert(true));
    }

    @Override
    public void setPageState(String url, PageCrawlState state) {
        pagesInfo.updateOne(
                Filters.eq("url", url),
                new Document("state", state.getValue()),
                new UpdateOptions().upsert(true));
    }
}
