package me.ymssd.dts.sink.sharding;

import io.shardingjdbc.core.api.algorithm.sharding.PreciseShardingValue;
import io.shardingjdbc.core.api.algorithm.sharding.standard.PreciseShardingAlgorithm;
import java.util.Collection;
import org.bson.types.ObjectId;

/**
 * @author denghui
 * @create 2018/9/18
 */
public class ModShardingAlgorithm<T extends Comparable<?>> implements PreciseShardingAlgorithm<T> {

    @Override
    public String doSharding(Collection<String> availableTargetNames, PreciseShardingValue<T> shardingValue) {
        int size = availableTargetNames.size();
        int index = 0;
        if (shardingValue.getValue() instanceof Number) {
            long value = ((Number) shardingValue.getValue()).longValue();
            index = (int) (value % size);
        } else if (shardingValue.getValue() instanceof String
            && ObjectId.isValid((String) shardingValue.getValue())) {
            String value = (String) shardingValue.getValue();
            index = value.charAt(value.length() - 1) % size;
        } else {
            throw new UnsupportedOperationException("un supported sharding value");
        }

        int i = 0;
        for (String availableTargetName : availableTargetNames) {
            if (i == index) {
                return availableTargetName;
            }
            i ++;
        }
        return null;
    }
}
