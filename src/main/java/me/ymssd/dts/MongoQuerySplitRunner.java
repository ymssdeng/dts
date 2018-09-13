package me.ymssd.dts;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.lte;

import com.google.common.collect.Range;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import me.ymssd.dts.config.DtsConfig.QueryConfig;
import me.ymssd.dts.model.Record;
import me.ymssd.dts.model.Split;
import org.bson.Document;
import org.bson.types.ObjectId;

/**
 * @author denghui
 * @create 2018/9/6
 */
@Slf4j
public class MongoQuerySplitRunner implements QuerySplitRunner {

    private QueryConfig queryConfig;
    private Metric metric;
    private MongoDatabase mongoDatabase;

    public MongoQuerySplitRunner(MongoClient mongoClient, QueryConfig queryConfig, Metric metric) {
        this.queryConfig = queryConfig;
        this.metric = metric;

        mongoDatabase = mongoClient.getDatabase(queryConfig.getMongo().getDatabase());
    }

    @Override
    public Range<String> getMinMaxId() {
        final String field = "_id";
        Document min = mongoDatabase.getCollection(queryConfig.getTable())
            .find()
            .projection(Projections.include(field))
            .sort(Sorts.ascending(field))
            .first();
        Document max = mongoDatabase.getCollection(queryConfig.getTable())
            .find()
            .projection(Projections.include(field))
            .sort(Sorts.descending(field))
            .first();
        return Range.closed(min.get(field).toString(), max.get(field).toString());
    }

    @Override
    public List<Range<String>> splitRange(Range<String> range) {
        int step = queryConfig.getStep();
        int lowerTime = Integer.valueOf(range.lowerEndpoint().substring(0, 8), 16);
        int upperTime = Integer.valueOf(range.upperEndpoint().substring(0, 8), 16);
        String suffix = range.lowerEndpoint().substring(8);
        List<Range<String>> ranges = new ArrayList<>();
        String lower = range.lowerEndpoint(), upper;
        while (lowerTime + step < upperTime) {
            lowerTime += step;
            upper = Integer.toHexString(lowerTime) + suffix;
            ranges.add(Range.closed(lower, upper));
            lower = upper;
        }
        upper = range.upperEndpoint();
        ranges.add(Range.closed(lower, upper));
        return ranges;
    }

    @Override
    public Split query(Range<String> range) {
        List<Record> records = new ArrayList<>();
        MongoCursor<Document> cursor = mongoDatabase.getCollection(queryConfig.getTable())
            .find(and(gte("_id", new ObjectId(range.lowerEndpoint())),
                lte("_id", new ObjectId(range.upperEndpoint()))))
            .iterator();
        while (cursor.hasNext()) {
            Document object = cursor.next();
            Record record = new Record();
            for (String key : object.keySet()) {
                record.add(new SimpleEntry<>(key, object.get(key)));
            }
            records.add(record);
        }
        Split split = new Split();
        split.setRange(range);
        split.setRecords(records);
        metric.getSize().addAndGet(split.getRecords().size());
        log.info("query split:{}", range);
        return split;
    }
}
