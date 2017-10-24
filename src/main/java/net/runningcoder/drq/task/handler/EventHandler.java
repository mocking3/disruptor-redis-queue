package net.runningcoder.drq.task.handler;

import net.runningcoder.drq.queue.EventQueue;

/**
 * Created by machine on 2017/10/22.
 */
public class EventHandler {

    public void onEvent(String id, String eventType, EventQueue queue) {
        try {
            //处理成功
            queue.success(id);
        } catch (Exception e) {
            queue.fail(id);
        }
    }
}
