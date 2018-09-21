package me.ymssd.dts.fetch;

import com.google.common.collect.Range;
import java.sql.SQLException;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import me.ymssd.dts.config.DtsConfig.FetchConfig;
import me.ymssd.dts.model.Record;
import me.ymssd.dts.model.Split;
import me.ymssd.dts.sink.AbstractMysqlSinker;
import me.ymssd.dts.util.MysqlUtils;
import org.apache.commons.dbutils.QueryRunner;

/**
 * @author denghui
 * @create 2018/9/19
 */
@Slf4j
public class SplitMysqlFetcher implements SplitFetcher<Long> {

    private DataSource dataSource;
    private FetchConfig fetchConfig;

    private String priColumnName;

    public SplitMysqlFetcher(DataSource dataSource, FetchConfig fetchConfig) throws SQLException {
        this.dataSource = dataSource;
        this.fetchConfig = fetchConfig;

        this.priColumnName = MysqlUtils.getPRIColumnName(dataSource, fetchConfig.getTable());
    }

    @Override
    public Range<Long> getMinMaxId() {
        try {
            StringBuilder sb = new StringBuilder("SELECT MIN(");
            sb.append(AbstractMysqlSinker.KEYWORD_ESCAPE);
            sb.append(priColumnName);
            sb.append(AbstractMysqlSinker.KEYWORD_ESCAPE);
            sb.append(") AS MinId, MAX(");
            sb.append(AbstractMysqlSinker.KEYWORD_ESCAPE);
            sb.append(priColumnName);
            sb.append(AbstractMysqlSinker.KEYWORD_ESCAPE);
            sb.append(") AS MaxId FROM ");
            sb.append(AbstractMysqlSinker.KEYWORD_ESCAPE);
            sb.append(fetchConfig.getTable());
            sb.append(AbstractMysqlSinker.KEYWORD_ESCAPE);

            QueryRunner runner = new QueryRunner(dataSource);
            return runner.query(sb.toString(), rs -> {
                rs.next();
                long minId = rs.getLong("MinId");
                long maxId = rs.getLong("MaxId");
                return Range.closed(minId, maxId);
            });
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<Range<Long>> splitRange(Range<Long> range) {
        long step = fetchConfig.getStep();
        List<Range<Long>> ranges = new ArrayList<>();
        long lower = range.lowerEndpoint(), upper;
        while (lower + step < range.upperEndpoint()) {
            upper = lower + step;
            ranges.add(Range.closed(lower, upper));
            lower = upper;
        }
        upper = range.upperEndpoint();
        ranges.add(Range.closed(lower, upper));
        return ranges;
    }

    @Override
    public Split query(Range<Long> range) {
        StringBuilder sb = new StringBuilder("SELECT * FROM ");
        sb.append(AbstractMysqlSinker.KEYWORD_ESCAPE);
        sb.append(fetchConfig.getTable());
        sb.append(AbstractMysqlSinker.KEYWORD_ESCAPE);
        sb.append(" WHERE ");
        sb.append(AbstractMysqlSinker.KEYWORD_ESCAPE);
        sb.append(priColumnName);
        sb.append(AbstractMysqlSinker.KEYWORD_ESCAPE);
        sb.append(" >= ");
        sb.append(range.lowerEndpoint());
        sb.append(" AND ");
        sb.append(AbstractMysqlSinker.KEYWORD_ESCAPE);
        sb.append(priColumnName);
        sb.append(AbstractMysqlSinker.KEYWORD_ESCAPE);
        sb.append(" <= ");
        sb.append(range.upperEndpoint());
        QueryRunner runner = new QueryRunner(dataSource);

        try {
            List<Record> records = runner.query(sb.toString(), rs -> {
                List<Record> result = new ArrayList<>();
                while (rs.next()) {
                    Record record = new Record();
                    for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                        record.add(new SimpleEntry<>(rs.getMetaData().getColumnName(i), rs.getObject(i)));
                    }
                    result.add(record);
                }
                return result;
            });

            Split split = new Split();
            split.setRecords(records);
            return split;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
