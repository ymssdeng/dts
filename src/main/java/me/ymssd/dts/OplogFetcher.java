package me.ymssd.dts;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gt;

import com.mongodb.CursorType;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Sorts;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
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

    public OplogFetcher(MongoClient mongoClient, FetchConfig fetchConfig) {
        this.fetchConfig = fetchConfig;
        this.mongoClient = mongoClient;
    }

    @Override
    public void run(Consumer<ReplicaLog> consumer) {
        MongoCollection<Document> collection = mongoClient.getDatabase("local")
            .getCollection("oplog.rs");
        MongoCursor<Document> cursor = collection.find()
            .sort(Sorts.descending("$natural"))
            .limit(1)
            .iterator();
        if (!cursor.hasNext()) {
            log.error("no oplog find");
            return;
        }

        Document oplog = cursor.next();
        BsonTimestamp ts = (BsonTimestamp) oplog.get("ts");
        String ns = fetchConfig.getMongo().getDatabase() + "." + fetchConfig.getTable();
        cursor = collection.find(and(eq("ns", ns), gt("ts", ts)))
            .cursorType(CursorType.TailableAwait)
            .noCursorTimeout(true)
            .iterator();
        log.info("start to fetch oplog, ns:{}, ts:{}", ns, ts);
        while (true) {
            oplog = cursor.tryNext();
            if (oplog != null) {
                log.debug("{}", oplog);
                ReplicaLog replicaLog = new ReplicaLog();
                OplogOp op = OplogOp.find(oplog.getString("op"));
                replicaLog.setOp(op);
                Record record = new Record();
                replicaLog.setRecord(record);
                if (op == OplogOp.INSERT) {
                    for (Entry<String, Object> entry : ((Document) oplog.get("o")).entrySet()) {
                        SimpleEntry field = new SimpleEntry(entry.getKey(), entry.getValue());
                        record.add(field);
                    }
                } else if (op == OplogOp.UPDATE) {

                }
                consumer.accept(replicaLog);
            }
        }
    }
}
