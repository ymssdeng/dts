package me.ymssd.dts.model;

import com.google.common.collect.Range;
import java.util.List;
import javax.sql.DataSource;
import lombok.Data;

/**
 * @author denghui
 * @create 2018/9/12
 */
@Data
public class SinkSplit {

    private DataSource dataSource;
    private String table;
    private Range<String> range;
    private List<Record> records;
}
