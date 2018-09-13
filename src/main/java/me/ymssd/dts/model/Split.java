package me.ymssd.dts.model;

import com.google.common.collect.Range;
import java.util.List;
import lombok.Data;

/**
 * @author denghui
 * @create 2018/9/13
 */
@Data
public class Split {

    private Range<String> range;
    private List<Record> records;
}
