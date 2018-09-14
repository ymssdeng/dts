package me.ymssd.dts;

import io.reactivex.Observable;
import io.reactivex.schedulers.Schedulers;
import java.sql.SQLException;
import lombok.extern.slf4j.Slf4j;
import me.ymssd.dts.config.DtsConfig;
import me.ymssd.dts.model.Record;
import me.ymssd.dts.model.ReplicaLog;

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
        .subscribeOn(Schedulers.from(sinkExecutor))
//        .observeOn(Schedulers.from(fetchExecutor))
        .map((obj -> {
            ReplicaLog replicaLog = (ReplicaLog) obj;
            Record record = fieldMapper.apply(replicaLog.getRecord());
            replicaLog.setRecord(record);
            return replicaLog;
        }))
        .filter(replicaLog -> replicaLog.getRecord() != null)
        .subscribe((replicaLog) -> replicaLogSinker.sink(replicaLog));
    }

    @Override
    protected void startDump() {

    }
}
