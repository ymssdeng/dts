package me.ymssd.dts.sink;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import me.ymssd.dts.Metric;
import me.ymssd.dts.config.DtsConfig.SinkConfig;
import me.ymssd.dts.model.Record;
import me.ymssd.dts.model.Split;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;

/**
 * @author denghui
 * @create 2018/9/6
 */
@Slf4j
public class SplitMysqlSinker extends AbstractMysqlSinker implements SplitSinker {

    public SplitMysqlSinker(DataSource dataSource, SinkConfig sinkConfig, Metric metric) throws SQLException {
        super(dataSource, sinkConfig, metric);
    }

    public void sink(Split split) {
        Connection connection = null;
        try {
            Object[][] params = new Object[split.getRecords().size()][];
            for (int i = 0; i < split.getRecords().size(); i++) {
                Record record = split.getRecords().get(i);

                Object[] param = new Object[cmdList.size()];
                for (int j = 0; j < cmdList.size(); j++) {
                    param[j] = record.getValue(cmdList.get(j).getField());
                    param[j] = Optional.ofNullable(param[j]).orElse(cmdList.get(j).getDefaultValue());
                }
                params[i] = param;
            }
            connection = dataSource.getConnection();
            connection.setAutoCommit(false);
            QueryRunner runner = new QueryRunner(dataSource);
            runner.batch(connection, insertSqlFormat, params);
            connection.commit();

            metric.getSinkSize().addAndGet(split.getRecords().size());
            log.info("sink size:{}", split.getRecords().size());
        } catch (SQLException e) {
            metric.getFailedRange().add(split.getRange());
            log.error("fail sink split", e);
        } finally {
            DbUtils.closeQuietly(connection);
        }
    }


}
