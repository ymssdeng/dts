package me.ymssd.dts.model;

import java.util.Date;
import lombok.Data;
import org.apache.commons.lang.StringUtils;

/**
 * @author denghui
 * @create 2018/9/19
 */
@Data
public class ColumnMetaData {

    private String Field;
    private String Type;
    private String Null;
    private String Key;
    private String Default;
    private String Extra;

    public Object getDefaultValue() {
        if ("CURRENT_TIMESTAMP".equals(Default)) {
            return new Date();
        }
        return Default;
    }

    public boolean isPrimaryKey() {
        return "PRI".equals(Key);
    }

    public boolean isAutoIncrement() {
        return StringUtils.isNotEmpty(Extra) && Extra.contains("auto_increment");
    }
}
