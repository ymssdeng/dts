package me.ymssd.dts;

import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import io.reactivex.Flowable;
import io.reactivex.Observable;
import io.reactivex.schedulers.Schedulers;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import me.ymssd.dts.config.DtsConfig;
import me.ymssd.dts.model.Record;
import me.ymssd.dts.model.ReplicaLog;
import me.ymssd.dts.model.Split;

/**
 * @author denghui
 * @create 2018/9/14
 */
@Slf4j
public class RxJavaDts extends AbstractDts {

    public RxJavaDts(DtsConfig dtsConfig) throws SQLException {
        super(dtsConfig);
    }

    @Override
    protected void startSync() {
        Observable.fromPublisher(subscriber -> {
            replicaLogFetcher.run((obj) -> subscriber.onNext(obj));
            })
            .subscribeOn(Schedulers.from(fetchExecutor))
            .flatMap(obj -> {
                ReplicaLog replicaLog = (ReplicaLog) obj;
                Record record = fieldMapper.apply(replicaLog.getRecord());
                replicaLog.setRecord(record);
                return Observable.just(replicaLog)
                    .observeOn(Schedulers.from(sinkExecutor));
            })
            .filter(replicaLog -> replicaLog.getRecord() != null)
            .subscribe((replicaLog) -> replicaLogSinker.sink(replicaLog));
    }

    @Override
    protected void startDump() {
        List<Range<String>> ranges = getRanges();
        Flowable.fromIterable(ranges)
            .parallel(fetchConfig.getThreadCount(), 1)
            .runOn(Schedulers.from(fetchExecutor))
            .flatMap(range -> {
                Split querySplit = splitFetcher.query(range);
                List<Record> mappedRecords = querySplit.getRecords().stream()
                    .map(r -> fieldMapper.apply(r))
                    .filter(r -> r != null)
                    .collect(Collectors.toList());
                List<Split> sinkSplits = Lists.partition(mappedRecords, sinkConfig.getBatchSize())
                    .stream()
                    .map(partitionRecords -> {
                        Split sinkSplit = new Split();
                        sinkSplit.setRecords(partitionRecords);
                        sinkSplit.setRange(querySplit.getRange());
                        return sinkSplit;
                    })
                    .collect(Collectors.toList());
                return Flowable.fromIterable(sinkSplits);
            })
            .sequential()
            .doOnComplete(() -> metric.setFetchEndTime(System.currentTimeMillis()))
            .parallel(sinkConfig.getThreadCount(), 1)
            .runOn(Schedulers.from(sinkExecutor))
            .flatMap(split -> {
                splitSinker.sink(split);
                if (metric.getSinkStartTime() == 0) {
                    metric.setSinkStartTime(System.currentTimeMillis());
                }
                return Flowable.empty();
            })
            .sequential()
            .doOnComplete(() -> {
                metric.setSinkEndTime(System.currentTimeMillis());
                print();
            })
            .subscribe();
    }
}
