package me.ymssd.dts;

import static java.time.Instant.ofEpochMilli;
import static java.time.LocalDateTime.ofInstant;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import me.ymssd.dts.config.DtsConfig;
import me.ymssd.dts.config.DtsConfig.QueryConfig;
import me.ymssd.dts.config.DtsConfig.SinkConfig;
import me.ymssd.dts.model.QuerySplit;
import me.ymssd.dts.model.Record;
import me.ymssd.dts.model.SinkSplit;

/**
 * @author denghui
 * @create 2018/9/12
 */
@Slf4j
public class Dts {
    private static final DateTimeFormatter YMDHMS= DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private QueryConfig queryConfig;
    private SinkConfig sinkConfig;
    private ExecutorService queryExecutor;
    private ExecutorService mapExecutor;
    private ExecutorService sinkExecutor;
    private QuerySplitRunner querySplitRunner;
    private FieldMapper fieldMapper;
    private SinkSplitRunner sinkSplitRunner;
    private DataSource sinkDataSource;
    private Metric metric;

    public Dts(DtsConfig dtsConfig) {
        Preconditions.checkNotNull(dtsConfig.getQuery());
        Preconditions.checkNotNull(dtsConfig.getSink());
        this.queryConfig = dtsConfig.getQuery();
        this.sinkConfig = dtsConfig.getSink();

        //query
        if (queryConfig.getMongo() != null) {
            MongoClient mongoClient = MongoClients.create(queryConfig.getMongo().getUrl());
            MongoDatabase mongoDatabase = mongoClient.getDatabase(queryConfig.getMongo().getDatabase());
            querySplitRunner = new MongoQuerySplitRunner(mongoDatabase);
        }
        Preconditions.checkNotNull(querySplitRunner);
        fieldMapper = new FieldMapper(dtsConfig.getMapping());

        //sink
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(sinkConfig.getUrl());
        hikariConfig.setUsername(sinkConfig.getUsername());
        hikariConfig.setPassword(sinkConfig.getPassword());
        sinkDataSource = new HikariDataSource(hikariConfig);
        sinkSplitRunner = new MysqlSinkSplitRunner();

        //线程池
        ThreadFactoryBuilder builder = new ThreadFactoryBuilder();
        builder.setNameFormat("query-runner-%d");
        queryExecutor = Executors.newFixedThreadPool(queryConfig.getThreadCount(), builder.build());
        builder.setNameFormat("map-runner-%d");
        mapExecutor = Executors.newFixedThreadPool(queryConfig.getThreadCount(), builder.build());
        builder.setNameFormat("sink-runner-%d");
        sinkExecutor = Executors.newFixedThreadPool(sinkConfig.getThreadCount(), builder.build());

        //metric
        metric = new Metric();
    }

    public void start() {
        metric.setQueryStartTime(System.currentTimeMillis());
        Range<String> range = querySplitRunner.getMinMaxId(queryConfig.getMinId(), queryConfig.getMaxId(), queryConfig.getTable());
        List<String> ids = querySplitRunner.splitId(range, queryConfig.getStep());

        List<CompletableFuture> queryFutures = new ArrayList<>();
        List<CompletableFuture> sinkFutures = new ArrayList<>();
        for (int i = 0; i < ids.size() - 1; i++) {
            final String lower = ids.get(i);
            final String upper = ids.get(i + 1);

            CompletableFuture future = CompletableFuture
                .supplyAsync(() -> {
                    QuerySplit querySplit = new QuerySplit();
                    querySplit.setRange(Range.closed(lower, upper));
                    querySplit.setTable(queryConfig.getTable());
                    return querySplitRunner.query(querySplit);
                }, queryExecutor)
                .thenApplyAsync(records -> {
                    return Lists.partition(records, sinkConfig.getBatchSize())
                        .stream()
                        .map(partitionRecords -> {
                            List<Record> mappedRecords = partitionRecords.stream()
                                .map(r -> fieldMapper.apply(r))
                                .filter(r -> r != null)
                                .collect(Collectors.toList());
                            if (mappedRecords.isEmpty()) {
                                return null;
                            }
                            return mappedRecords;
                        })
                        .filter(r -> r != null)
                        .collect(Collectors.toList());
                }, mapExecutor)
                .thenAcceptAsync(splits -> {
                    for (List<Record> split : splits) {
                        SinkSplit sinkSplit = new SinkSplit();
                        sinkSplit.setRecords(split);
                        sinkSplit.setDataSource(sinkDataSource);
                        sinkSplit.setTable(sinkConfig.getTable());
                        sinkFutures.add(CompletableFuture.runAsync(() -> sinkSplitRunner.sink(sinkSplit), sinkExecutor));
                        if (metric.getSinkStartTime() == 0) {
                            metric.setSinkStartTime(System.currentTimeMillis());
                        }
                    }
                }, sinkExecutor);
            queryFutures.add(future);
        }

        CompletableFuture.allOf(queryFutures.toArray(new CompletableFuture[0]))
            .whenComplete((v, t) -> {
                metric.setQueryEndTime(System.currentTimeMillis());
                CompletableFuture.allOf(sinkFutures.toArray(new CompletableFuture[0]))
                    .whenComplete((v2, t2) -> {
                        metric.setSinkEndTime(System.currentTimeMillis());
                        print();

                        System.exit(0);
                    });
            });
    }

    private void print() {
        ZoneId zoneId = ZoneId.systemDefault();
        log.info("-->queryStartTime:{}", YMDHMS.format(ofInstant(ofEpochMilli(metric.getQueryStartTime()), zoneId)));
        log.info("-->queryEndTime:{}", YMDHMS.format(ofInstant(ofEpochMilli(metric.getQueryEndTime()), zoneId)));
        log.info("-->size:{}", metric.getSize());
        log.info("-->sinkStartTime:{}", YMDHMS.format(ofInstant(ofEpochMilli(metric.getSinkStartTime()), zoneId)));
        log.info("-->sinkEndTime:{}", YMDHMS.format(ofInstant(ofEpochMilli(metric.getSinkEndTime()), zoneId)));
        log.info("-->sankSize:{}", metric.getSankSize());
    }
}
