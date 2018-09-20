package me.ymssd.dts.sink;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Optional;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import me.ymssd.dts.config.DtsConfig.SinkConfig;
import me.ymssd.dts.model.Record;
import me.ymssd.dts.model.ReplicaLog;
import org.apache.commons.dbutils.DbUtils;

/**
 * @author denghui
 * @create 2018/9/13
 */
@Slf4j
public class ReplicaLogMysqlSinker extends AbstractMysqlSinker implements ReplicaLogSinker {

    public ReplicaLogMysqlSinker(DataSource noShardingDataSource, DataSource dataSource,
        SinkConfig sinkConfig) throws SQLException {
        super(noShardingDataSource, dataSource, sinkConfig);
    }

    @Override
    public void sink(ReplicaLog replicaLog) {
        Connection connection = null;
        PreparedStatement stmt = null;
        try {
            connection = dataSource.getConnection();
            stmt = connection.prepareStatement(insertSqlFormat);

            Record record = replicaLog.getRecord();
            for (int j = 0; j < cmdList.size(); j++) {
                if (cmdList.get(j).isPrimaryKey()) {
                    stmt.setObject(j + 1, keyGenerator.generateKey().longValue());
                } else {
                    Object obj = record.getValue(cmdList.get(j).getField());
                    obj = Optional.ofNullable(obj).orElse(cmdList.get(j).getDefaultValue());
                    stmt.setObject(j + 1, obj);
                }
            }

            stmt.execute();
            log.info("sink replica log:{}", record);
        } catch (SQLException e) {
            log.error("fail to sink replica log", e);
        } finally {
            DbUtils.closeQuietly(stmt);
            DbUtils.closeQuietly(connection);
        }
    }
}
