package me.ymssd.dts.model;

import lombok.Data;
import me.ymssd.dts.OplogOp;

/**
 * @author denghui
 * @create 2018/9/13
 */
@Data
public class ReplicaLog {

    private OplogOp op;
    private Record record;
}
