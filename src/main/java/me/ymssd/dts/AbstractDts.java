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
import io.shardingjdbc.core.api.ShardingDataSourceFactory;
import io.shardingjdbc.core.api.config.ShardingRuleConfiguration;
import io.shardingjdbc.core.api.config.TableRuleConfiguration;
import io.shardingjdbc.core.api.config.strategy.ShardingStrategyConfiguration;
import io.shardingjdbc.core.api.config.strategy.StandardShardingStrategyConfiguration;
import java.sql.SQLException;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import me.ymssd.dts.config.DtsConfig;
import me.ymssd.dts.config.DtsConfig.FetchConfig;
import me.ymssd.dts.config.DtsConfig.Mode;
import me.ymssd.dts.config.DtsConfig.SinkConfig;
import me.ymssd.dts.fetch.FieldMapper;
import me.ymssd.dts.fetch.OplogFetcher;
import me.ymssd.dts.fetch.ReplicaLogFetcher;
import me.ymssd.dts.fetch.SplitFetcher;
import me.ymssd.dts.fetch.SplitMongoFetcher;
import me.ymssd.dts.sink.ReplicaLogMysqlSinker;
import me.ymssd.dts.sink.ReplicaLogSinker;
import me.ymssd.dts.sink.SplitMysqlSinker;
import me.ymssd.dts.sink.SplitSinker;

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
    protected ExecutorService sinkExecutor;
    protected SplitFetcher splitFetcher;
    protected FieldMapper fieldMapper;
    protected SplitSinker splitSinker;
    protected DataSource sinkDataSource;
    protected HikariDataSource noShardingSinkDataSource;
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
            if (dtsConfig.getMode() == Mode.Dump) {
                splitFetcher = new SplitMongoFetcher(mongoClient, fetchConfig, metric);
            } else if (dtsConfig.getMode() == Mode.Sync) {
                replicaLogFetcher = new OplogFetcher(mongoClient, fetchConfig, metric);
            }
        }
        fieldMapper = new FieldMapper(dtsConfig.getMapping());

        //sink
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(sinkConfig.getUrl());
        hikariConfig.setUsername(sinkConfig.getUsername());
        hikariConfig.setPassword(sinkConfig.getPassword());
        noShardingSinkDataSource = new HikariDataSource(hikariConfig);
        if (sinkConfig.isShardingMode()) {
            ShardingRuleConfiguration src = new ShardingRuleConfiguration();
            TableRuleConfiguration trc = new TableRuleConfiguration();
            trc.setLogicTable(sinkConfig.getLogicTable()); //T_Order_${0..1}
            trc.setActualDataNodes("ds." + sinkConfig.getTables());

            ShardingStrategyConfiguration ssc = new StandardShardingStrategyConfiguration(
                sinkConfig.getShardingColumn(),
                sinkConfig.getShardingStrategy());
            trc.setTableShardingStrategyConfig(ssc);
            src.getTableRuleConfigs().add(trc);
            Map<String, DataSource> dsMap = new HashMap<>();
            dsMap.put("ds", noShardingSinkDataSource);
            sinkDataSource = ShardingDataSourceFactory.createDataSource(dsMap, src, new HashMap<>(), new Properties());
        } else {
            sinkDataSource = noShardingSinkDataSource;
        }
        if (dtsConfig.getMode() == Mode.Dump) {
            splitSinker = new SplitMysqlSinker(noShardingSinkDataSource, sinkDataSource, sinkConfig, metric);
        } else if (dtsConfig.getMode() == Mode.Sync) {
            replicaLogSinker = new ReplicaLogMysqlSinker(noShardingSinkDataSource, sinkDataSource, sinkConfig, metric);
        }

        //线程池
        ThreadFactoryBuilder builder = new ThreadFactoryBuilder();
        builder.setNameFormat("fetch-runner-%d");
        fetchExecutor = Executors.newFixedThreadPool(fetchConfig.getThreadCount(), builder.build());
        builder.setNameFormat("sink-runner-%d");
        sinkExecutor = Executors.newFixedThreadPool(sinkConfig.getThreadCount(), builder.build());
        builder.setNameFormat("shutdown-hook");
        ThreadFactory shutdownHookFactory = builder.build();
        Runtime.getRuntime().addShutdownHook(shutdownHookFactory.newThread(() -> {
            fetchExecutor.shutdown();
            sinkExecutor.shutdown();
            mongoClient.close();
            noShardingSinkDataSource.close();
        }));
    }

    public void start() {
        metric.setFetchStartTime(System.currentTimeMillis());
        if (dtsConfig.getMode() == Mode.Dump) {
            startDump();
        } else if (dtsConfig.getMode() == Mode.Sync) {
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

    protected void printMetric() {
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
