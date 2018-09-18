package me.ymssd.dts.sink;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import io.shardingjdbc.core.keygen.DefaultKeyGenerator;
import io.shardingjdbc.core.keygen.KeyGenerator;
import io.shardingjdbc.core.util.InlineExpressionParser;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import lombok.Data;
import me.ymssd.dts.Metric;
import me.ymssd.dts.config.DtsConfig.SinkConfig;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.lang.StringUtils;

/**
 * @author denghui
 * @create 2018/9/14
 */
public abstract class AbstractMysqlSinker {

    public static final String KEYWORD_ESCAPE = "`";

    protected DataSource noShardingDataSource;
    protected DataSource dataSource;
    protected SinkConfig sinkConfig;
    protected Metric metric;

    protected List<ColumnMetaData> cmdList;
    protected List<String> columnNames;
    protected String insertSqlFormat;
    protected KeyGenerator keyGenerator;

    public AbstractMysqlSinker(DataSource noShardingDataSource, DataSource dataSource, SinkConfig sinkConfig, Metric metric) throws SQLException {
        this.noShardingDataSource = noShardingDataSource;
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

        keyGenerator = new DefaultKeyGenerator();
    }

    protected List<ColumnMetaData> getColumnMetaData() throws SQLException {
        String table = null;
        if (StringUtils.isNotEmpty(sinkConfig.getShardingColumn())
            && StringUtils.isNotEmpty(sinkConfig.getShardingStrategy())
            && StringUtils.isNotEmpty(sinkConfig.getActualTables())) {
            List<String> actualTables = new InlineExpressionParser(sinkConfig.getActualTables()).evaluate();
            table = actualTables.get(0);
        } else {
            table = sinkConfig.getTable();
        }

        StringBuilder sb = new StringBuilder("SHOW COLUMNS FROM ");
        sb.append(KEYWORD_ESCAPE);
        sb.append(table);
        sb.append(KEYWORD_ESCAPE);
        QueryRunner runner = new QueryRunner(noShardingDataSource);
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
                    cmdList.add(cmd);
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
