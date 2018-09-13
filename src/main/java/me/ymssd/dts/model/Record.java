package me.ymssd.dts.model;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;

/**
 * @author denghui
 * @create 2018/9/6
 */
public class Record extends ArrayList<SimpleEntry<String, Object>> {

    public Object getValue(String key) {
        for (SimpleEntry<String, Object> field : this) {
            if (key.equals(field.getKey())) {
                return field.getValue();
            }
        }
        return null;
    }
}
