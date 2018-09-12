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

    Range<String> getMinMaxId(String defaultMinId, String defaultMaxId, String table);

    List<String> splitId(Range<String> range, int step);

    List<Record> query(QuerySplit split);
}
