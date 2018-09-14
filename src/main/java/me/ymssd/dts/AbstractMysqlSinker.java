package me.ymssd.dts;

import com.google.common.base.Joiner;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import lombok.Data;
import me.ymssd.dts.config.DtsConfig.SinkConfig;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.lang.StringUtils;

/**
 * @author denghui
 * @create 2018/9/14
 */
public abstract class AbstractMysqlSinker {

    public static final String KEYWORD_ESCAPE = "`";

    protected DataSource dataSource;
    protected SinkConfig sinkConfig;
    protected Metric metric;

    protected List<ColumnMetaData> cmdList;
    protected List<String> columnNames;
    protected String insertSqlFormat;

    public AbstractMysqlSinker(DataSource dataSource, SinkConfig sinkConfig, Metric metric) throws SQLException {
        this.dataSource = dataSource;
        this.sinkConfig = sinkConfig;
        this.metric = metric;

        cmdList = getColumnMetaData();
        columnNames = cmdList.stream().map(cmd -> cmd.getField()).collect(Collectors.toList());

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
        insertSqlFormat = sb.toString();
    }

    protected List<ColumnMetaData> getColumnMetaData() throws SQLException {
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
