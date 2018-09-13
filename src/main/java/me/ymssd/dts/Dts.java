package me.ymssd.dts;

import static java.time.Instant.ofEpochMilli;
import static java.time.LocalDateTime.ofInstant;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.SQLException;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import me.ymssd.dts.config.DtsConfig;
import me.ymssd.dts.config.DtsConfig.FetchConfig;
import me.ymssd.dts.config.DtsConfig.Mode;
import me.ymssd.dts.config.DtsConfig.SinkConfig;
import me.ymssd.dts.model.Record;
import me.ymssd.dts.model.Split;

/**
 * @author denghui
 * @create 2018/9/12
 */
@Slf4j
public class Dts {
    private static final DateTimeFormatter YMDHMS= DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private DtsConfig dtsConfig;
    private FetchConfig fetchConfig;
    private SinkConfig sinkConfig;
    private ExecutorService queryExecutor;
    private ExecutorService mapExecutor;
    private ExecutorService sinkExecutor;
    private SplitFetcher splitFetcher;
    private FieldMapper fieldMapper;
    private SplitSinker splitSinker;
    private HikariDataSource sinkDataSource;
    private MongoClient mongoClient;
    private Metric metric;
    private ReplicaLogFetcher replicaLogFetcher;
    private ReplicaLogSinker replicaLogSinker;

    public Dts(DtsConfig dtsConfig) throws SQLException {
        Preconditions.checkNotNull(dtsConfig.getMode());
        Preconditions.checkNotNull(dtsConfig.getFetch());
        Preconditions.checkNotNull(dtsConfig.getSink());
        this.dtsConfig = dtsConfig;
        this.fetchConfig = dtsConfig.getFetch();
        this.sinkConfig = dtsConfig.getSink();
        //metric
        metric = new Metric();

        //fetch
        if (fetchConfig.getMongo() != null) {
            mongoClient = MongoClients.create(fetchConfig.getMongo().getUrl());
            if (dtsConfig.getMode() == Mode.dump) {
                splitFetcher = new SplitMongoFetcher(mongoClient, fetchConfig, metric);
            } else if (dtsConfig.getMode() == Mode.sync) {
                replicaLogFetcher = new OplogFetcher(mongoClient, fetchConfig);
            }
        }
        fieldMapper = new FieldMapper(dtsConfig.getMapping());

        //sink
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(sinkConfig.getUrl());
        hikariConfig.setUsername(sinkConfig.getUsername());
        hikariConfig.setPassword(sinkConfig.getPassword());
        sinkDataSource = new HikariDataSource(hikariConfig);
        if (dtsConfig.getMode() == Mode.dump) {
            splitSinker = new SplitMysqlSinker(sinkDataSource, sinkConfig, metric);
        } else if (dtsConfig.getMode() == Mode.sync) {
            replicaLogSinker = new ReplicaLogMysqlSinker();
        }

        //线程池
        ThreadFactoryBuilder builder = new ThreadFactoryBuilder();
        builder.setNameFormat("fetch-runner-%d");
        queryExecutor = Executors.newFixedThreadPool(fetchConfig.getThreadCount(), builder.build());
        builder.setNameFormat("map-runner-%d");
        mapExecutor = Executors.newFixedThreadPool(fetchConfig.getThreadCount(), builder.build());
        builder.setNameFormat("sink-runner-%d");
        sinkExecutor = Executors.newFixedThreadPool(sinkConfig.getThreadCount(), builder.build());
    }

    public void start() {
        metric.setFetchStartTime(System.currentTimeMillis());
        if (dtsConfig.getMode() == Mode.dump) {
            startDump();
        } else if (dtsConfig.getMode() == Mode.sync) {
            startSync();
        }
    }

    private void startSync() {
        CompletableFuture.runAsync(() -> {
            replicaLogFetcher.run(replicaLog ->
                CompletableFuture.runAsync(() -> replicaLogSinker.sink(replicaLog), sinkExecutor));
        }, queryExecutor);
    }

    private void startDump() {
        List<Range<String>> ranges = fetchConfig.getRangeList();
        if (ranges == null) {
            Range<String> range = splitFetcher.getMinMaxId();
            ranges = splitFetcher.splitRange(range);
        }

        List<CompletableFuture> queryFutures = new ArrayList<>();
        List<CompletableFuture> sinkFutures = new ArrayList<>();
        for (Range<String> range : ranges) {
            final String lower = range.lowerEndpoint();
            final String upper = range.upperEndpoint();

            CompletableFuture future = CompletableFuture
                .supplyAsync(() -> splitFetcher.query(Range.closed(lower, upper)), queryExecutor)
                .thenApplyAsync(querySplit -> {
                    return Lists.partition(querySplit.getRecords(), sinkConfig.getBatchSize())
                        .stream()
                        .map(partitionRecords -> {
                            List<Record> mappedRecords = partitionRecords.stream()
                                .map(r -> fieldMapper.apply(r))
                                .filter(r -> r != null)
                                .collect(Collectors.toList());
                            if (mappedRecords.isEmpty()) {
                                return null;
                            }
                            Split sinkSplit = new Split();
                            sinkSplit.setRecords(mappedRecords);
                            sinkSplit.setRange(querySplit.getRange());
                            return sinkSplit;
                        })
                        .filter(r -> r != null)
                        .collect(Collectors.toList());
                }, mapExecutor)
                .thenAcceptAsync(sinkSplits -> {
                    for (Split sinkSplit : sinkSplits) {
                        sinkFutures.add(CompletableFuture.runAsync(() -> splitSinker.sink(sinkSplit), sinkExecutor));
                        if (metric.getSinkStartTime() == 0) {
                            metric.setSinkStartTime(System.currentTimeMillis());
                        }
                    }
                }, sinkExecutor);
            queryFutures.add(future);
        }

        CompletableFuture.allOf(queryFutures.toArray(new CompletableFuture[0]))
            .whenComplete((v, t) -> {
                metric.setFetchEndTime(System.currentTimeMillis());
                CompletableFuture.allOf(sinkFutures.toArray(new CompletableFuture[0]))
                    .whenComplete((v2, t2) -> {
                        metric.setSinkEndTime(System.currentTimeMillis());
                        print();

                        mongoClient.close();
                        sinkDataSource.close();
                        System.exit(0);
                    });
            });
    }

    private void print() {
        ZoneId zoneId = ZoneId.systemDefault();
        log.info("-->fetchStartTime:{}", YMDHMS.format(ofInstant(ofEpochMilli(metric.getFetchStartTime()), zoneId)));
        log.info("-->fetchEndTime:{}", YMDHMS.format(ofInstant(ofEpochMilli(metric.getFetchEndTime()), zoneId)));
        log.info("-->fetchSize:{}", metric.getFetchSize());
        log.info("-->sinkStartTime:{}", YMDHMS.format(ofInstant(ofEpochMilli(metric.getSinkStartTime()), zoneId)));
        log.info("-->sinkEndTime:{}", YMDHMS.format(ofInstant(ofEpochMilli(metric.getSinkEndTime()), zoneId)));
        log.info("-->sinkSize:{}", metric.getSinkSize());
    }
}
