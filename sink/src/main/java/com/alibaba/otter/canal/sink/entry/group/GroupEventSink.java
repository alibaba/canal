package com.alibaba.otter.canal.sink.entry.group;

import java.util.Arrays;
import java.util.List;

import com.alibaba.otter.canal.sink.CanalEventDownStreamHandler;
import com.alibaba.otter.canal.sink.entry.EntryEventSink;
import com.alibaba.otter.canal.store.model.Event;

/**
 * 基于归并排序的sink处理
 * 
 * <pre>
 * 几点设计说明：
 * 1. 多库合并时，需要控制不满足groupSize的条件，就会阻塞其他库的合并操作.  (比如刚启动时会所有通道正常工作才开始合并，或者中间过程出现主备切换)
 * 2. 库解析出现问题，但没有进行主备切换，此时需要通过{@linkplain CanalEventDownStreamHandler}进行定时监听合并数据的产生时间间隔 
 *    a. 因为一旦库解析异常，就不会再sink数据，此时groupSize就会一直缺少，就会阻塞其他库的合并，也就是不会有数据写入到store中
 * </pre>
 * 
 * @author jianghang 2012-10-15 下午09:54:18
 * @version 1.0.0
 */
public class GroupEventSink extends EntryEventSink {

    private int          groupSize;
    private GroupBarrier barrier;  // 归并排序需要预先知道组的大小，用于判断是否组内所有的sink都已经开始正常取数据

    public GroupEventSink(){
        this(1);
    }

    public GroupEventSink(int groupSize){
        super();
        this.groupSize = groupSize;
    }

    public void start() {
        super.start();

        if (filterTransactionEntry) {
            barrier = new TimelineBarrier(groupSize);
        } else {
            barrier = new TimelineTransactionBarrier(groupSize);// 支持事务保留
        }
    }

    protected boolean doSink(List<Event> events) {
        int size = events.size();
        for (int i = 0; i < events.size(); i++) {
            Event event = events.get(i);
            try {
                barrier.await(event);// 进行timeline的归并调度处理
                if (filterTransactionEntry) {
                    super.doSink(Arrays.asList(event));
                } else if (i == size - 1) {
                    // 针对事务数据，只有到最后一条数据都通过后，才进行sink操作，保证原子性
                    // 同时批量sink，也要保证在最后一条数据释放状态之前写出数据，否则就有并发问题
                    return super.doSink(events);
                }
            } catch (InterruptedException e) {
                return false;
            } finally {
                barrier.clear(event);
            }
        }

        return false;
    }

    public void interrupt() {
        super.interrupt();
        barrier.interrupt();
    }

}
