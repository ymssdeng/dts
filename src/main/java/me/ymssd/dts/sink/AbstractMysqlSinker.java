package me.ymssd.dts.sink;

import com.google.common.base.Joiner;
import io.shardingjdbc.core.keygen.DefaultKeyGenerator;
import io.shardingjdbc.core.keygen.KeyGenerator;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import me.ymssd.dts.Metric;
import me.ymssd.dts.config.DtsConfig.SinkConfig;
import me.ymssd.dts.model.ColumnMetaData;
import me.ymssd.dts.util.MysqlUtils;

/**
 * @author denghui
 * @create 2018/9/14
 */
public abstract class AbstractMysqlSinker {

    public static final String KEYWORD_ESCAPE = "`";

    protected DataSource noShardingDataSource;
    protected DataSource dataSource;
    protected SinkConfig sinkConfig;

    protected List<ColumnMetaData> cmdList;
    protected List<String> columnNames;
    protected String insertSqlFormat;
    protected KeyGenerator keyGenerator;

    public AbstractMysqlSinker(DataSource noShardingDataSource, DataSource dataSource, SinkConfig sinkConfig) throws SQLException {
        this.noShardingDataSource = noShardingDataSource;
        this.dataSource = dataSource;
        this.sinkConfig = sinkConfig;

        cmdList = MysqlUtils.getColumnsMetaData(noShardingDataSource, sinkConfig.getLogicTable());
        Iterator<ColumnMetaData> iterator = cmdList.iterator();
        while (iterator.hasNext()) {
            ColumnMetaData columnMetaData = iterator.next();
            if (columnMetaData.isPrimaryKey() && columnMetaData.isAutoIncrement()) {
                iterator.remove();
            }
        }
        columnNames = cmdList.stream().map(cmd -> cmd.getField()).collect(Collectors.toList());

        StringBuilder sb = new StringBuilder();
        sb.append("INSERT IGNORE INTO ");
        sb.append(KEYWORD_ESCAPE);
        sb.append(sinkConfig.getLogicTable());
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

}
