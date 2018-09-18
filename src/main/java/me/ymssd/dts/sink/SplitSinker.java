package me.ymssd.dts.sink;

import me.ymssd.dts.model.Split;

/**
 * @author denghui
 * @create 2018/9/10
 */
public interface SplitSinker {

    void sink(Split split);

}
