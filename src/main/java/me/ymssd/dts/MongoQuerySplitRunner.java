package me.ymssd.dts;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.lte;

import com.google.common.collect.Range;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import me.ymssd.dts.model.QuerySplit;
import me.ymssd.dts.model.Record;
import org.apache.commons.lang.StringUtils;
import org.bson.Document;
import org.bson.types.ObjectId;

/**
 * @author denghui
 * @create 2018/9/6
 */
@Slf4j
public class MongoQuerySplitRunner implements QuerySplitRunner {

    private MongoDatabase mongoDatabase;

    public MongoQuerySplitRunner(MongoDatabase mongoDatabase) {
        this.mongoDatabase = mongoDatabase;
    }

    @Override
    public Range<String> getMinMaxId(String defaultMinId, String defaultMaxId, String collection) {
        String minId = defaultMinId;
        String maxId = defaultMaxId;
        if (StringUtils.isEmpty(minId)) {
            Document min = mongoDatabase.getCollection(collection)
                .find()
                .projection(Projections.include("_id"))
                .sort(Sorts.ascending("_id"))
                .first();
            minId = min.get("_id").toString();
        }
        if (StringUtils.isEmpty(maxId)) {
            Document max = mongoDatabase.getCollection(collection)
                .find()
                .projection(Projections.include("_id"))
                .sort(Sorts.descending("_id"))
                .first();
            maxId = max.get("_id").toString();
        }

        return Range.closed(minId, maxId);
    }

    @Override
    public List<String> splitId(Range<String> range, int step) {
        int lowerTime = Integer.valueOf(range.lowerEndpoint().substring(0, 8), 16);
        int upperTime = Integer.valueOf(range.upperEndpoint().substring(0, 8), 16);
        String suffix = range.lowerEndpoint().substring(8);
        List<String> ids = new ArrayList<>();
        ids.add(range.lowerEndpoint());
        while (lowerTime + step < upperTime) {
            lowerTime += step;
            ids.add(Integer.toHexString(lowerTime) + suffix);
        }
        ids.add(range.upperEndpoint());
        return ids;
    }

    public List<Record> query(QuerySplit split) {
        List<Record> records = new ArrayList<>();
        MongoCursor<Document> cursor = mongoDatabase.getCollection(split.getTable())
            .find(and(gte("_id", new ObjectId(split.getRange().lowerEndpoint())),
                lte("_id", new ObjectId(split.getRange().upperEndpoint()))))
            .iterator();
        while (cursor.hasNext()) {
            Document object = cursor.next();
            Record record = new Record();
            for (String key : object.keySet()) {
                record.add(new SimpleEntry<>(key, object.get(key)));
            }
            records.add(record);
        }
        log.info("query split:{}", split.getRange());
        return records;
    }
}
