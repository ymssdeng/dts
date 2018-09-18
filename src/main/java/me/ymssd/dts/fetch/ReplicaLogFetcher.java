package me.ymssd.dts.fetch;

import java.util.function.Consumer;
import me.ymssd.dts.model.ReplicaLog;

/**
 * @author denghui
 * @create 2018/9/13
 */
public interface ReplicaLogFetcher {

    void run(Consumer<ReplicaLog> consumer);
}
