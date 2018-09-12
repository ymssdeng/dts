package me.ymssd.dts.model;

import com.google.common.collect.Range;
import lombok.Data;

/**
 * @author denghui
 * @create 2018/9/12
 */
@Data
public class QuerySplit {

    private String table;
    private Range<String> range;
}
