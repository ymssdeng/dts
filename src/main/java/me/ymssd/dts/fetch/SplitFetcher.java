package me.ymssd.dts.fetch;

import com.google.common.collect.Range;
import java.util.List;
import me.ymssd.dts.model.Split;

/**
 * @author denghui
 * @create 2018/9/10
 */
public interface SplitFetcher {

    Range<String> getMinMaxId();

    List<Range<String>> splitRange(Range<String> range);

    Split query(Range<String> range);

}
