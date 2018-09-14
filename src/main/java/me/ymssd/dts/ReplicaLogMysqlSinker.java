package me.ymssd.dts;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import me.ymssd.dts.config.DtsConfig.SinkConfig;
import me.ymssd.dts.model.Record;
import me.ymssd.dts.model.ReplicaLog;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;

/**
 * @author denghui
 * @create 2018/9/13
 */
@Slf4j
public class ReplicaLogMysqlSinker extends AbstractMysqlSinker implements ReplicaLogSinker {

    public ReplicaLogMysqlSinker(DataSource dataSource, SinkConfig sinkConfig, Metric metric) throws SQLException {
        super(dataSource, sinkConfig, metric);
    }

    @Override
    public void sink(ReplicaLog replicaLog) {
        Connection connection = null;
        try {
            Record record = replicaLog.getRecord();
            Object[] params = new Object[cmdList.size()];
            for (int j = 0; j < cmdList.size(); j++) {
                params[j] = record.getValue(cmdList.get(j).getField());
                params[j] = Optional.ofNullable(params[j]).orElse(cmdList.get(j).getDefaultValue());
            }
            connection = dataSource.getConnection();
            QueryRunner runner = new QueryRunner(dataSource);
            runner.execute(insertSqlFormat, params);
            metric.getFetchSize().incrementAndGet();
            log.info("sink replica log:{}", record);
        } catch (SQLException e) {
            log.error("fail to sink replica log", e);
        } finally {
            DbUtils.closeQuietly(connection);
        }
    }
}
