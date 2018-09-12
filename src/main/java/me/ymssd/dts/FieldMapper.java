package me.ymssd.dts;

import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import me.ymssd.dts.model.Record;

/**
 * @author denghui
 * @create 2018/9/6
 */
public class FieldMapper implements Function<Record, Record> {

    private Map<String, String> mapping = new HashMap<>();
    private Map<String, Function> converters = new HashMap<>();

    public FieldMapper(Map<String, String> mapping) {
        if (mapping != null && !mapping.isEmpty()) {
            this.mapping = mapping;
        }

        addValueConverter("_id", ValueConverters.TO_STRING_CONVERTER);
    }

    public void addValueConverter(String field, Function converter) {
        converters.put(field, converter);
    }

    @Override
    public Record apply(Record record) {
        Record mappedRecord = new Record();
        for (SimpleEntry<String, Object> field : record) {
            String mappedKey = mapping.get(field.getKey());
            if (mappedKey != null) {
                Object value = field.getValue();
                if (converters.containsKey(field.getKey())) {
                    value = converters.get(field.getKey()).apply(field.getValue());
                }
                if (value != null) {
                    mappedRecord.add(new SimpleEntry<>(mappedKey.toString(), value));
                }
            }
        }
        return mappedRecord.isEmpty() ? null : mappedRecord;
    }
}
