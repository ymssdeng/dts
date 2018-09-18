package me.ymssd.dts.sink;

import me.ymssd.dts.model.ReplicaLog;

/**
 * @author denghui
 * @create 2018/9/13
 */
public interface ReplicaLogSinker {

    void sink(ReplicaLog replicaLog);
}
