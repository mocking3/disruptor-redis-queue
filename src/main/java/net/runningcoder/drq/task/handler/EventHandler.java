package net.runningcoder.drq.task.handler;

/**
 * Created by machine on 2017/10/22.
 */
public interface EventHandler<T> {
    void onEvent(T obj);
}
