package me.ymssd.dts;

import com.google.common.base.Joiner;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import me.ymssd.dts.config.DtsConfig.SinkConfig;
import me.ymssd.dts.model.Record;
import me.ymssd.dts.model.Split;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.lang.StringUtils;

/**
 * @author denghui
 * @create 2018/9/6
 */
@Slf4j
public class SplitMysqlSinker implements SplitSinker {

    public static final String KEYWORD_ESCAPE = "`";

    private DataSource dataSource;
    private SinkConfig sinkConfig;
    private Metric metric;

    private List<ColumnMetaData> cmdList;
    private List<String> columnNames;

    public SplitMysqlSinker(DataSource dataSource, SinkConfig sinkConfig, Metric metric) throws SQLException {
        this.dataSource = dataSource;
        this.sinkConfig = sinkConfig;
        this.metric = metric;

        cmdList = getColumnMetaData();
        columnNames = cmdList.stream().map(cmd -> cmd.getField()).collect(Collectors.toList());
    }

    public void sink(Split split) {
        Connection connection = null;
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("INSERT IGNORE INTO ");
            sb.append(KEYWORD_ESCAPE);
            sb.append(sinkConfig.getTable());
            sb.append(KEYWORD_ESCAPE);
            sb.append(" (");
            sb.append(KEYWORD_ESCAPE);
            sb.append(Joiner.on(KEYWORD_ESCAPE + ',' + KEYWORD_ESCAPE).join(columnNames));
            sb.append(KEYWORD_ESCAPE);
            sb.append(") VALUES (");
            String[] placeholders = new String[cmdList.size()];
            Arrays.fill(placeholders, "?");
            sb.append(Joiner.on(", ").join(Arrays.asList(placeholders)));
            sb.append(")");

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
            runner.batch(connection, sb.toString(), params);
            connection.commit();

            metric.getSinkSize().addAndGet(split.getRecords().size());
            log.info("sink fetchSize:{}", split.getRecords().size());
        } catch (SQLException e) {
            metric.getFailedRange().add(split.getRange());
            log.error("fail sink split", e);
        } finally {
            DbUtils.closeQuietly(connection);
        }
    }

    private List<ColumnMetaData> getColumnMetaData() throws SQLException {
        StringBuilder sb = new StringBuilder("SHOW COLUMNS FROM ");
        sb.append(KEYWORD_ESCAPE);
        sb.append(sinkConfig.getTable());
        sb.append(KEYWORD_ESCAPE);
        QueryRunner runner = new QueryRunner(dataSource);
        return runner.query(sb.toString(), rs -> {
            List<ColumnMetaData> cmdList = new ArrayList<>();
            while (rs.next()) {
                ColumnMetaData cmd = new ColumnMetaData();
                cmd.setField(rs.getString("Field"));
                cmd.setType(rs.getString("Type"));
                cmd.setNull(rs.getString("Null"));
                cmd.setKey(rs.getString("Key"));
                cmd.setDefault(rs.getString("Default"));
                cmd.setExtra(rs.getString("Extra"));
                if (!cmd.isPrimaryKey()) {
                    cmdList.add(cmd);
                } else if (!cmd.isAutoIncrement()) {
                    throw new RuntimeException("support auto increment PK only");
                }
            }
            return cmdList;
        });
    }

    @Data
    static class ColumnMetaData {
        private String Field;
        private String Type;
        private String Null;
        private String Key;
        private String Default;
        private String Extra;

        public Object getDefaultValue() {
            if ("CURRENT_TIMESTAMP".equals(Default)) {
                return new Date();
            }
            return Default;
        }

        public boolean isPrimaryKey() {
            return "PRI".equals(Key);
        }

        public boolean isAutoIncrement() {
            return StringUtils.isNotEmpty(Extra) && Extra.contains("auto_increment");
        }
    }
}
