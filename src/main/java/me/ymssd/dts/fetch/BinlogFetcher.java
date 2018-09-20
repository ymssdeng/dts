package me.ymssd.dts.fetch;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.google.common.base.Splitter;
import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.function.Consumer;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import me.ymssd.dts.config.DtsConfig.FetchConfig;
import me.ymssd.dts.model.Record;
import me.ymssd.dts.model.ReplicaLog;
import me.ymssd.dts.util.MysqlUtils;

/**
 * @author denghui
 * @create 2018/9/20
 */
@Slf4j
public class BinlogFetcher implements ReplicaLogFetcher {

    private DataSource dataSource;
    private FetchConfig fetchConfig;
    private long tableId = -1;
    private List<String> columnNames;

    public BinlogFetcher(DataSource dataSource, FetchConfig fetchConfig) throws SQLException {
        this.dataSource = dataSource;
        this.fetchConfig = fetchConfig;
        this.columnNames = MysqlUtils.getColumnNames(dataSource, fetchConfig.getTable());
    }

    @Override
    public void run(Consumer<ReplicaLog> consumer) {
        String jdbcUrl = fetchConfig.getMysql().getUrl();
        String hostPort = jdbcUrl.substring(13, 13 + jdbcUrl.substring(13).indexOf('/'));
        List<String> segments = Splitter.on(':').splitToList(hostPort);

        BinaryLogClient client = new BinaryLogClient(segments.get(0), Integer.valueOf(segments.get(1)),
            fetchConfig.getMysql().getUsername(),
            fetchConfig.getMysql().getPassword());
        if (fetchConfig.getMysql().getPosition() > 0) {
            client.setBinlogPosition(fetchConfig.getMysql().getPosition());
        }
        client.setServerId(System.currentTimeMillis());
        client.registerEventListener(event -> {
            EventType eventType = event.getHeader().getEventType();
            if (eventType == EventType.TABLE_MAP) {
                TableMapEventData data = event.getData();
                if (data.getTable().equals(fetchConfig.getTable())) {
                    tableId = data.getTableId();
                }
            } else if (tableId > 0 && EventType.isRowMutation(eventType)) {
                if (EventType.isWrite(eventType)) {
                    WriteRowsEventData data = event.getData();
                    if (data.getTableId() != tableId) {
                        return;
                    }
                    for (Serializable[] row : data.getRows()) {
                        ReplicaLog replicaLog = new ReplicaLog();
                        Record record = new Record();
                        for (int i = 0; i < row.length; i++) {
                            record.add(new SimpleEntry<>(columnNames.get(i), row[i]));
                        }
                        replicaLog.setRecord(record);
                        replicaLog.setOp(ReplicaLogOp.INSERT);
                        consumer.accept(replicaLog);
                    }
                }
            }
        });
        try {
            client.connect();
        } catch (IOException e) {
            if (client != null && client.isConnected()) {
                try {
                    client.disconnect();
                } catch (IOException e2) { }
            }
            log.error("binlog client error", e);
            System.exit(1);
        }
    }
}
