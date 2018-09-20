package me.ymssd.dts.model;

import lombok.Data;
import me.ymssd.dts.fetch.ReplicaLogOp;

/**
 * @author denghui
 * @create 2018/9/13
 */
@Data
public class ReplicaLog {

    private ReplicaLogOp op;
    private Record record;
}
