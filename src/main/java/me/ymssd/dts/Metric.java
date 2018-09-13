package me.ymssd.dts;

import com.google.common.collect.Range;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Data;

/**
 * @author denghui
 * @create 2018/9/12
 */
@Data
public class Metric {

    private long fetchStartTime;
    private long fetchEndTime;
    private AtomicLong fetchSize = new AtomicLong(0);

    private long sinkStartTime;
    private long sinkEndTime;
    private AtomicLong sinkSize = new AtomicLong(0);
    private List<Range<String>> failedRange = new ArrayList<>();

    public void reset() {
        setFetchStartTime(0);
        setFetchEndTime(0);
        getFetchSize().set(0);
        setSinkStartTime(0);
        setSinkEndTime(0);
        getSinkSize().set(0);
        failedRange.clear();
    }
}
