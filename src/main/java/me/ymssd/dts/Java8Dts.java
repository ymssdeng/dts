package me.ymssd.dts;

import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import me.ymssd.dts.config.DtsConfig;
import me.ymssd.dts.model.Record;
import me.ymssd.dts.model.Split;

/**
 * @author denghui
 * @create 2018/9/14
 */
@Slf4j
public class Java8Dts extends AbstractDts {

    public Java8Dts(DtsConfig dtsConfig) throws SQLException {
        super(dtsConfig);
    }

    @Override
    protected void startSync() {
        CompletableFuture.runAsync(() -> {
            replicaLogFetcher.run(replicaLog ->
                CompletableFuture.runAsync(() -> {
                    Record mappedRecord = fieldMapper.apply(replicaLog.getRecord());
                    if (mappedRecord == null) {
                        return;
                    }
                    replicaLog.setRecord(mappedRecord);
                    replicaLogSinker.sink(replicaLog);
                }, sinkExecutor));
        }, fetchExecutor);
    }

    @Override
    protected void startDump() {
        List<Range<String>> ranges = getRanges();

        List<CompletableFuture> queryFutures = new ArrayList<>();
        List<CompletableFuture> sinkFutures = new ArrayList<>();
        for (Range<String> range : ranges) {
            CompletableFuture future = CompletableFuture
                .supplyAsync(() -> {
                    Split querySplit = splitFetcher.query(range);
                    List<Record> mappedRecords = querySplit.getRecords().stream()
                        .map(r -> fieldMapper.apply(r))
                        .filter(r -> r != null)
                        .collect(Collectors.toList());
                    return Lists.partition(mappedRecords, sinkConfig.getBatchSize())
                        .stream()
                        .map(partitionRecords -> {
                            Split sinkSplit = new Split();
                            sinkSplit.setRecords(partitionRecords);
                            sinkSplit.setRange(querySplit.getRange());
                            return sinkSplit;
                        })
                        .collect(Collectors.toList());
                }, fetchExecutor)
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

                        System.exit(0);
                    });
            });
    }
}
