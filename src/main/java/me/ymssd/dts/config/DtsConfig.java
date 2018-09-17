package me.ymssd.dts.config;

import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import java.util.List;
import java.util.Map;
import lombok.Data;
import lombok.NoArgsConstructor;
import me.ymssd.dts.AbstractDts.Style;

/**
 * @author denghui
 * @create 2018/9/6
 */
@Data
@NoArgsConstructor
public class DtsConfig {

    private Style style = Style.Java8;
    private Mode mode;
    private FetchConfig fetch;
    private SinkConfig sink;
    private Map<String, String> mapping;

    @Data
    @NoArgsConstructor
    public static class FetchConfig {

        private MongoDataSource mongo;
        private MysqlDataSource mysql;
        private String table;
        private List<String> range;
        private int step = 10000;
        private int threadCount = 1;

        public Range<String> getInputRange() {
            if (range == null) {
                return null;
            }
            Preconditions.checkArgument(range.size() == 2);
            return Range.closed(range.get(0), range.get(1));
        }

        @Data
        @NoArgsConstructor
        public static class MongoDataSource {
            private String url;
            private String database;
            private int startTime;
            private int endTime;
        }

        @Data
        @NoArgsConstructor
        public static class MysqlDataSource {
            private String url;
        }
    }

    @Data
    @NoArgsConstructor
    public static class SinkConfig {
        private String url;
        private String username;
        private String password;
        private String table;
        private int threadCount = 1;
        private int batchSize = 100;
    }

    public enum Mode {
        dump, sync
    }
}
