package me.ymssd.dts.fetch;

import com.google.common.collect.Range;
import java.util.List;
import me.ymssd.dts.model.Split;

/**
 * @author denghui
 * @create 2018/9/10
 */
public interface SplitFetcher<T extends Comparable<?>> {

    Range<T> getMinMaxId();

    List<Range<T>> splitRange(Range<T> range);

    Split query(Range<T> range);

}
