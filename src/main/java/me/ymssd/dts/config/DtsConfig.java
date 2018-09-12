package me.ymssd.dts.config;

import java.util.Map;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author denghui
 * @create 2018/9/6
 */
@Data
@NoArgsConstructor
public class DtsConfig {

    private QueryConfig query;
    private SinkConfig sink;
    private Map<String, String> mapping;

    @Data
    @NoArgsConstructor
    public static class QueryConfig {

        private MongoDataSource mongo;
        private MysqlDataSource mysql;
        private String table;
        private String minId;
        private String maxId;
        private int step = 10000;
        private int threadCount = 5;

        @Data
        @NoArgsConstructor
        public static class MongoDataSource {
            private String url;
            private String database;
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
        private int threadCount = 5;
        private int batchSize = 100;
    }

}
