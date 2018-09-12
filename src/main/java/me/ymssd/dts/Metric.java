package me.ymssd.dts;

import java.util.concurrent.atomic.AtomicLong;
import lombok.Data;

/**
 * @author denghui
 * @create 2018/9/12
 */
@Data
public class Metric {

    private long queryStartTime;
    private long queryEndTime;
    private AtomicLong size = new AtomicLong(0);

    private long sinkStartTime;
    private long sinkEndTime;
    private AtomicLong sankSize = new AtomicLong(0);

    public void reset() {
        setQueryStartTime(0);
        setQueryEndTime(0);
        getSize().set(0);
        setSinkStartTime(0);
        setSinkEndTime(0);
        getSankSize().set(0);
    }
}
