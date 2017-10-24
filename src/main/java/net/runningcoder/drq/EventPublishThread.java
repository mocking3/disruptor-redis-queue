package net.runningcoder.drq;

import com.lmax.disruptor.dsl.Disruptor;
import lombok.Data;
import net.runningcoder.drq.queue.EventQueue;

/**
 * Created by machine on 2017/10/22.
 */
@Data
public class EventPublishThread extends Thread {
    private String eventType;
    private EventQueue eventQueue;
    private Disruptor<Event> disruptor;
    private boolean running;

    public EventPublishThread(String eventType, EventQueue eventQueue, Disruptor<Event> disruptor) {
        this.eventType = eventType;
        this.eventQueue = eventQueue;
        this.disruptor = disruptor;
        this.running = true;
    }

    public void run() {
        while (running) {
            String nextKey = null;
            try {
                if (nextKey == null) {
                    nextKey = eventQueue.next();
                }
                if (nextKey != null) {
                    disruptor.publishEvent((event, sequence, key, eventType) -> {
                        event.setKey(key);
                        event.setType(eventType);
                    }, nextKey, eventType);
                }
            } catch (Exception e) {
//                log.error(nextKey, e);
            }
        }
    }

    public void shutdown() {
        running = false;
    }
}
