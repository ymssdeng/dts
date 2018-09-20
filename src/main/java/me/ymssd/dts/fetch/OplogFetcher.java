package me.ymssd.dts.fetch;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gt;

import com.mongodb.CursorType;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Sorts;
import java.time.Instant;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import me.ymssd.dts.Metric;
import me.ymssd.dts.config.DtsConfig.FetchConfig;
import me.ymssd.dts.model.Record;
import me.ymssd.dts.model.ReplicaLog;
import org.bson.BsonTimestamp;
import org.bson.Document;

/**
 * @author denghui
 * @create 2018/9/13
 */
@Slf4j
public class OplogFetcher implements ReplicaLogFetcher {

    private FetchConfig fetchConfig;
    private MongoClient mongoClient;
    private Metric metric;

    public OplogFetcher(MongoClient mongoClient, FetchConfig fetchConfig, Metric metric) {
        this.fetchConfig = fetchConfig;
        this.mongoClient = mongoClient;
        this.metric = metric;
    }

    private MongoCollection<Document> getOplogCollection() {
        return mongoClient.getDatabase("local").getCollection("oplog.rs");
    }

    private BsonTimestamp getStartTs() {
        if (fetchConfig.getMongo().getStartTime() > 0) {
            return new BsonTimestamp(fetchConfig.getMongo().getStartTime(), 1);
        }
        MongoCursor<Document> cursor = getOplogCollection()
            .find()
            .sort(Sorts.descending("$natural"))
            .limit(1)
            .iterator();
        if (!cursor.hasNext()) {
            throw new RuntimeException("no oplog findByMongoCode");
        }
        Document oplog = cursor.next();
        BsonTimestamp ts = (BsonTimestamp) oplog.get("ts");
        return ts;
    }

    @Override
    public void run(Consumer<ReplicaLog> consumer) {
        /**
         * https://docs.mongodb.com/manual/core/tailable-cursors/
         * Because tailable cursors do not use indexes, the initial scan for
         * the query may be expensive; but, after initially exhausting the cursor,
         * subsequent retrievals of the newly added documents are inexpensive.
         */
        BsonTimestamp ts = getStartTs();
        String ns = fetchConfig.getMongo().getDatabase() + "." + fetchConfig.getTable();
        log.info("initial scan tailable cursor, ns:{}, ts:{}", ns, ts);

        MongoCursor<Document> cursor = getOplogCollection()
            .find(and(eq("ns", ns), gt("ts", ts)))
            .cursorType(CursorType.TailableAwait)
            .noCursorTimeout(true)
            .iterator();
        log.info("initial scan finished, start to fetch oplog");
        while (true) {
            int endTime = fetchConfig.getMongo().getEndTime();
            if (endTime > 0 && Instant.now().getEpochSecond() >= endTime) {
                log.info("time to exit oplog fetcher");
                break;
            }
            Document oplog = cursor.tryNext();
            if (oplog != null) {
                log.debug("{}", oplog);
                metric.getFetchSize().incrementAndGet();
                ReplicaLogOp op = ReplicaLogOp.findByMongoCode(oplog.getString("op"));
                Record record = new Record();
                Document data = null, o = (Document) oplog.get("o"), o2 = (Document) oplog.get("o2");
                if (op == ReplicaLogOp.INSERT) {
                    data = o;
                } else if (op == ReplicaLogOp.UPDATE) {
                    //data = (Document) o.get("$set");
                    //record.add(new SimpleEntry<>("_id", o2.getObjectId("_id")));
                }
                if (data != null) {
                    for (Entry<String, Object> entry : data.entrySet()) {
                        SimpleEntry field = new SimpleEntry(entry.getKey(), entry.getValue());
                        record.add(field);
                    }
                    ReplicaLog replicaLog = new ReplicaLog();
                    replicaLog.setOp(op);
                    replicaLog.setRecord(record);
                    consumer.accept(replicaLog);
                }
            }
        }
        System.exit(0);
    }
}
