package me.ymssd.dts;

import lombok.extern.slf4j.Slf4j;
import me.ymssd.dts.model.ReplicaLog;

/**
 * @author denghui
 * @create 2018/9/13
 */
@Slf4j
public class ReplicaLogMysqlSinker implements ReplicaLogSinker {

    @Override
    public void sink(ReplicaLog replicaLog) {
        log.info(replicaLog.toString());
    }
}
