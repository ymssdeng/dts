package me.ymssd.dts.fetch;

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
import me.ymssd.dts.Metric;
import me.ymssd.dts.config.DtsConfig.FetchConfig;
import me.ymssd.dts.model.Record;
import me.ymssd.dts.model.Split;
import org.bson.Document;
import org.bson.types.ObjectId;

/**
 * @author denghui
 * @create 2018/9/6
 */
@Slf4j
public class SplitMongoFetcher implements SplitFetcher<String> {

    private FetchConfig fetchConfig;
    private MongoClient mongoClient;

    public SplitMongoFetcher(MongoClient mongoClient, FetchConfig fetchConfig) {
        this.mongoClient = mongoClient;
        this.fetchConfig = fetchConfig;
    }

    @Override
    public Range<String> getMinMaxId() {
        final String field = "_id";
        MongoDatabase db = mongoClient.getDatabase(fetchConfig.getMongo().getDatabase());
        Document min = db.getCollection(fetchConfig.getTable())
            .find()
            .projection(Projections.include(field))
            .sort(Sorts.ascending(field))
            .first();
        Document max = db.getCollection(fetchConfig.getTable())
            .find()
            .projection(Projections.include(field))
            .sort(Sorts.descending(field))
            .first();
        return Range.closed(min.get(field).toString(), max.get(field).toString());
    }

    @Override
    public List<Range<String>> splitRange(Range<String> range) {
        int step = fetchConfig.getStep();
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
        MongoDatabase db = mongoClient.getDatabase(fetchConfig.getMongo().getDatabase());
        MongoCursor<Document> cursor = db.getCollection(fetchConfig.getTable())
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
        split.setRecords(records);
        return split;
    }

}
