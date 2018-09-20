package me.ymssd.dts.fetch;

/**
 * @author denghui
 * @create 2018/9/6
 */
public enum ReplicaLogOp {
    INSERT("i"), UPDATE("u"), DELETE("d");

    private final char mongoCode;

    ReplicaLogOp(String mongoCode) {
        this.mongoCode = mongoCode.charAt(0);
    }

    public char getMongoCode() {
        return mongoCode;
    }

    public static ReplicaLogOp findByMongoCode(String code) {
        if (code == null || code.length() == 0) {
            return null;
        }
        for (ReplicaLogOp value : ReplicaLogOp.values()) {
            if (value.getMongoCode() == code.charAt(0)) {
                return value;
            }
        }
        return null;
    }
}
