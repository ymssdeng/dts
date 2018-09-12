package me.ymssd.dts;

/**
 * @author denghui
 * @create 2018/9/6
 */
public enum OplogOperation {
    NO_OP("n"), INSERT("i"), UPDATE("u"), DELETE("d"), UNKNOWN("!");

    private final char code;

    OplogOperation(String code) {
        this.code = code.charAt(0);
    }

    public char getCode() {
        return code;
    }

    public static OplogOperation find(String code) {
        if (code == null || code.length() == 0) {
            return OplogOperation.UNKNOWN;
        }
        for (OplogOperation value : OplogOperation.values()) {
            if (value.getCode() == code.charAt(0)) {
                return value;
            }
        }
        return OplogOperation.UNKNOWN;
    }
}
