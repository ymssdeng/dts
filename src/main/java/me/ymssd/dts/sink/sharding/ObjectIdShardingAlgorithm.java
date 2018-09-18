package me.ymssd.dts.sink.sharding;

import io.shardingjdbc.core.api.algorithm.sharding.PreciseShardingValue;
import io.shardingjdbc.core.api.algorithm.sharding.standard.PreciseShardingAlgorithm;
import java.util.Collection;

/**
 * @author denghui
 * @create 2018/9/18
 */
public class ObjectIdShardingAlgorithm implements PreciseShardingAlgorithm<String> {

    @Override
    public String doSharding(Collection<String> availableTargetNames, PreciseShardingValue<String> shardingValue) {
        int size = availableTargetNames.size();
        int index = shardingValue.getValue().charAt(shardingValue.getValue().length() - 1) % size;
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
