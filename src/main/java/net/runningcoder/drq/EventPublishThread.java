package net.runningcoder.drq;

import com.lmax.disruptor.dsl.Disruptor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import net.runningcoder.drq.queue.EventQueue;

/**
 * Created by machine on 2017/10/22.
 */
@Slf4j
@Data
public class EventPublishThread extends Thread {
    private EventQueue eventQueue;
    private Disruptor<Event> disruptor;
    private boolean running;

    public EventPublishThread(EventQueue eventQueue, Disruptor<Event> disruptor) {
        this.eventQueue = eventQueue;
        this.disruptor = disruptor;
        this.running = true;
    }

    public void run() {
        while (running) {
            Object next = eventQueue.next();
            if (next != null) {
                disruptor.publishEvent((event, sequence, value) -> event.setObj(value), next);
            }
        }
    }

    public void shutdown() {
        running = false;
    }
}
