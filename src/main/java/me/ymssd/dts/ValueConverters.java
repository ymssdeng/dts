package me.ymssd.dts;


import java.util.function.Function;

/**
 * @author denghui
 * @create 2018/9/10
 */
public class ValueConverters {

    public static final Function<Object, String> TO_STRING_CONVERTER = o -> o.toString();

    public static final Function<Object, String> ALWAYS_NULL_CONVERTER = o -> null;
}
