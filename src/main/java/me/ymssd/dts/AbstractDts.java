package me.ymssd.dts;

import static java.time.Instant.ofEpochMilli;
import static java.time.LocalDateTime.ofInstant;

import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.SQLException;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import lombok.extern.slf4j.Slf4j;
import me.ymssd.dts.config.DtsConfig;
import me.ymssd.dts.config.DtsConfig.FetchConfig;
import me.ymssd.dts.config.DtsConfig.Mode;
import me.ymssd.dts.config.DtsConfig.SinkConfig;

/**
 * @author denghui
 * @create 2018/9/12
 */
@Slf4j
public abstract class AbstractDts {
    private static final DateTimeFormatter YMDHMS= DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    protected DtsConfig dtsConfig;
    protected FetchConfig fetchConfig;
    protected SinkConfig sinkConfig;
    protected ExecutorService fetchExecutor;
    protected ExecutorService mapExecutor;
    protected ExecutorService sinkExecutor;
    protected SplitFetcher splitFetcher;
    protected FieldMapper fieldMapper;
    protected SplitSinker splitSinker;
    protected HikariDataSource sinkDataSource;
    protected MongoClient mongoClient;
    protected Metric metric;
    protected ReplicaLogFetcher replicaLogFetcher;
    protected ReplicaLogSinker replicaLogSinker;

    public AbstractDts(DtsConfig dtsConfig) throws SQLException {
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
                replicaLogFetcher = new OplogFetcher(mongoClient, fetchConfig, metric);
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
            replicaLogSinker = new ReplicaLogMysqlSinker(sinkDataSource, sinkConfig, metric);
        }

        //线程池
        ThreadFactoryBuilder builder = new ThreadFactoryBuilder();
        builder.setNameFormat("fetch-runner-%d");
        fetchExecutor = Executors.newFixedThreadPool(fetchConfig.getThreadCount(), builder.build());
        builder.setNameFormat("map-runner-%d");
        mapExecutor = Executors.newFixedThreadPool(fetchConfig.getThreadCount(), builder.build());
        builder.setNameFormat("sink-runner-%d");
        sinkExecutor = Executors.newFixedThreadPool(sinkConfig.getThreadCount(), builder.build());
        builder.setNameFormat("shutdown-hook");
        ThreadFactory shutdownHookFactory = builder.build();
        Runtime.getRuntime().addShutdownHook(shutdownHookFactory.newThread(() -> mongoClient.close()));
        Runtime.getRuntime().addShutdownHook(shutdownHookFactory.newThread(() -> sinkDataSource.close()));
    }

    public void start() {
        metric.setFetchStartTime(System.currentTimeMillis());
        if (dtsConfig.getMode() == Mode.dump) {
            startDump();
        } else if (dtsConfig.getMode() == Mode.sync) {
            startSync();
        }
    }

    protected abstract void startSync();

    protected List<Range<String>> getRanges() {
        Range<String> range = fetchConfig.getInputRange();
        if (range == null) {
            range = splitFetcher.getMinMaxId();
        }
        return splitFetcher.splitRange(range);
    }

    protected abstract void startDump();

    protected void print() {
        ZoneId zoneId = ZoneId.systemDefault();
        log.info("-->fetchStartTime:{}", YMDHMS.format(ofInstant(ofEpochMilli(metric.getFetchStartTime()), zoneId)));
        log.info("-->fetchEndTime:{}", YMDHMS.format(ofInstant(ofEpochMilli(metric.getFetchEndTime()), zoneId)));
        log.info("-->fetchSize:{}", metric.getFetchSize());
        log.info("-->sinkStartTime:{}", YMDHMS.format(ofInstant(ofEpochMilli(metric.getSinkStartTime()), zoneId)));
        log.info("-->sinkEndTime:{}", YMDHMS.format(ofInstant(ofEpochMilli(metric.getSinkEndTime()), zoneId)));
        log.info("-->sinkSize:{}", metric.getSinkSize());
    }

    public enum Style {
        Java8, RxJava
    }
}
