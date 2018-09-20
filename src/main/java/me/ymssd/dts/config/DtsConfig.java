package me.ymssd.dts.config;

import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import io.shardingjdbc.core.util.InlineExpressionParser;
import java.util.List;
import java.util.Map;
import lombok.Data;
import lombok.NoArgsConstructor;
import me.ymssd.dts.AbstractDts.Style;
import org.apache.commons.lang.StringUtils;

/**
 * @author denghui
 * @create 2018/9/6
 */
@Data
@NoArgsConstructor
public class DtsConfig {

    private Style style = Style.Java8;
    private Mode mode = Mode.Dump;
    private FetchConfig fetch;
    private SinkConfig sink;
    private Map<String, String> mapping;

    @Data
    @NoArgsConstructor
    public static class FetchConfig {

        private MongoFetchConfig mongo;
        private MysqlFetchConfig mysql;
        private String table;
        private List<String> range;
        private int step = 10000;
        private int threadCount = Runtime.getRuntime().availableProcessors();

        public Range<String> getInputRange() {
            if (range == null) {
                return null;
            }
            Preconditions.checkArgument(range.size() == 2);
            return Range.closed(range.get(0), range.get(1));
        }

        @Data
        @NoArgsConstructor
        public static class MongoFetchConfig {
            private String url;
            private String database;
            private int startTime;
            private int endTime;
        }

        @Data
        @NoArgsConstructor
        public static class MysqlFetchConfig {
            private String url;
            private String username;
            private String password;
            private long position;
        }
    }

    @Data
    @NoArgsConstructor
    public static class SinkConfig {
        private String url;
        private String username;
        private String password;
        private String tables;
        private int threadCount = Runtime.getRuntime().availableProcessors();
        private int batchSize = 100;
        private String shardingColumn;
        private String shardingStrategy;

        public boolean isShardingMode() {
            return StringUtils.isNotEmpty(shardingColumn) && StringUtils.isNotEmpty(shardingStrategy);
        }

        public String getLogicTable() {
            return new InlineExpressionParser(tables).evaluate().get(0);
        }
    }

    public enum Mode {
        Dump, Sync
    }
}
