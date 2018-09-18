package me.ymssd.dts.fetch;

/**
 * @author denghui
 * @create 2018/9/6
 */
public enum OplogOp {
    NO_OP("n"), INSERT("i"), UPDATE("u"), DELETE("d"), UNKNOWN("!");

    private final char code;

    OplogOp(String code) {
        this.code = code.charAt(0);
    }

    public char getCode() {
        return code;
    }

    public static OplogOp find(String code) {
        if (code == null || code.length() == 0) {
            return OplogOp.UNKNOWN;
        }
        for (OplogOp value : OplogOp.values()) {
            if (value.getCode() == code.charAt(0)) {
                return value;
            }
        }
        return OplogOp.UNKNOWN;
    }
}
