package me.ymssd.dts.config;

import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import java.util.ArrayList;
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
        private List<List<String>> ranges;
        private int step = 10000;
        private int threadCount = 1;

        public List<Range<String>> getRangeList() {
            if (ranges == null) {
                return null;
            }
            List<Range<String>> result = new ArrayList<>();
            for (List<String> item : ranges) {
                Preconditions.checkNotNull(item);
                Preconditions.checkArgument(item.size() == 2);
                result.add(Range.closed(item.get(0), item.get(1)));
            }
            return result;
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
