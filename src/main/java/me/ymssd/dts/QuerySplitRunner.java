package me.ymssd.dts;

import com.google.common.collect.Range;
import java.util.List;
import me.ymssd.dts.model.Split;

/**
 * @author denghui
 * @create 2018/9/10
 */
public interface QuerySplitRunner {

    Range<String> getMinMaxId();

    List<String> splitId(Range<String> range);

    Split query(Range<String> range);
}
