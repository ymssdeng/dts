package me.ymssd.dts;

import com.google.common.base.Joiner;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import me.ymssd.dts.model.Record;
import me.ymssd.dts.model.SinkSplit;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;

/**
 * @author denghui
 * @create 2018/9/6
 */
@Slf4j
public class MysqlSinkSplitRunner implements SinkSplitRunner {

    public static final String KEYWORD_ESCAPE = "`";

    public void sink(SinkSplit split) {
        Connection connection = null;
        try {
            List<String> columns = getColumnNames(split);
            StringBuilder sb = new StringBuilder();
            sb.append("INSERT IGNORE INTO ");
            sb.append(KEYWORD_ESCAPE);
            sb.append(split.getTable());
            sb.append(KEYWORD_ESCAPE);
            sb.append(" (");
            sb.append(KEYWORD_ESCAPE);
            sb.append(Joiner.on(KEYWORD_ESCAPE + ',' + KEYWORD_ESCAPE).join(columns));
            sb.append(KEYWORD_ESCAPE);
            sb.append(") VALUES (");
            String[] placeholders = new String[columns.size()];
            Arrays.fill(placeholders, "?");
            sb.append(Joiner.on(", ").join(Arrays.asList(placeholders)));
            sb.append(")");

            Object[][] params = new Object[split.getRecords().size()][];
            for (int i = 0; i < split.getRecords().size(); i++) {
                Record record = split.getRecords().get(i);

                Object[] param = new Object[record.size()];
                for (int j = 0; j < record.size(); j++) {
                    param[j] = record.get(j).getValue();
                    params[i] = param;
                }
            }
            connection = split.getDataSource().getConnection();
            connection.setAutoCommit(false);
            QueryRunner runner = new QueryRunner(split.getDataSource());
            runner.batch(connection, sb.toString(), params);
            connection.commit();

            log.info("sink size:{}", split.getRecords().size());
        } catch (SQLException e) {
            log.error("fail sink split", e);
        } finally {
            DbUtils.closeQuietly(connection);
        }

    }

    private List<String> getColumnNames(SinkSplit split) throws SQLException {
        StringBuilder sb = new StringBuilder("SELECT * FROM ");
        sb.append(KEYWORD_ESCAPE);
        sb.append(split.getTable());
        sb.append(KEYWORD_ESCAPE);
        sb.append(" WHERE 1=2 limit 1");

        QueryRunner runner = new QueryRunner(split.getDataSource());
        return runner.query(sb.toString(), rs -> {
            List<String> names = new ArrayList<>();
            int count = rs.getMetaData().getColumnCount();
            for (int i = 1; i <= count; i++) {
                names.add(rs.getMetaData().getColumnName(i));
            }
            return names;
        });
    }
}
