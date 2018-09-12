package me.ymssd.dts;

import com.google.common.collect.Range;
import java.util.List;
import me.ymssd.dts.model.QuerySplit;
import me.ymssd.dts.model.Record;

/**
 * @author denghui
 * @create 2018/9/10
 */
public interface QuerySplitRunner {

    Range<String> getMinMaxId();

    List<String> splitId(Range<String> range);

    QuerySplit query(Range<String> range);
}
