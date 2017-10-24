package net.runningcoder.drq.task;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.WorkHandler;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import lombok.Data;
import net.runningcoder.drq.Event;
import net.runningcoder.drq.EventPublishThread;
import net.runningcoder.drq.queue.EventQueue;
import net.runningcoder.drq.task.handler.EventHandler;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

/**
 * Created by machine on 2017/10/22.
 */
@Data
public class EventWorker {
    private int bufferSize = 1024;
    private int workerSize = 64;
    private Map<EventQueue, EventHandler> eventHandlerMap;
    private Map<String, EventQueue> eventQueueMap;

    private Disruptor<Event> disruptor;

    private List<EventPublishThread> eventPublishThreads = Lists.newArrayList();

    public EventWorker(Map<EventQueue, EventHandler> eventHandlerMap, int bufferSize, int workerSize) {
        this.eventHandlerMap = eventHandlerMap;
        this.bufferSize = bufferSize;
        this.workerSize = workerSize;
    }

    public void init() {
        //1. 创建Disruptor
        disruptor = new Disruptor<>(
                Event::new,
                bufferSize,
                Executors.defaultThreadFactory(),
                ProducerType.MULTI,
                new BlockingWaitStrategy()
        );

        //2. 处理异常
        disruptor.setDefaultExceptionHandler(new ExceptionHandler<Event>() {
            public void handleEventException(Throwable throwable, long l, Event event) {

            }

            public void handleOnStartException(Throwable throwable) {

            }

            public void handleOnShutdownException(Throwable throwable) {

            }
        });

        //3. 创建工作者处理器
        WorkHandler<Event> workHandler = event -> {
            String type = event.getType();
            //3.1 根据事件类型获取 EventQueue
            EventQueue queue = eventQueueMap.get(type);
            //3.2 根据 EventQueue 获取该队列的事件处理器
            EventHandler handler = eventHandlerMap.get(queue);
            //3.3 交由 EventHandler 处理该事件
            handler.onEvent(event.getKey(), type, queue);
        };

        //4.1 创建工作者处理器
        WorkHandler[] workHandlers = new WorkHandler[workerSize];
        for (int i = 0; i < workerSize; i++) {
            workHandlers[i] = workHandler;
        }

        //4.2 告知 Disruptor 由工作者处理器处理
        disruptor.handleEventsWithWorkerPool(workHandlers);

        //5 启动 Disruptor
        disruptor.start();

        //6. 启动发布者线程
        for (Map.Entry<String, EventQueue> eventQueueEntry : eventQueueMap.entrySet()) {
            String eventType = eventQueueEntry.getKey();
            EventQueue eventQueue = eventQueueEntry.getValue();
            //
            EventPublishThread thread = new EventPublishThread(eventType, eventQueue, disruptor);
            eventPublishThreads.add(thread);
            thread.start();
        }

    }

    public void stop() {
        //1. 停止发布者线程
        for (EventPublishThread thread : eventPublishThreads) {
            thread.shutdown();
        }
        //2. 停止 Disruptor
        disruptor.shutdown();

    }

    public void setEventHandlerMap(Map<EventQueue, EventHandler> eventHandlerMap) {
        this.eventHandlerMap = eventHandlerMap;
        if (eventHandlerMap != null && !eventHandlerMap.isEmpty()) {
            this.eventQueueMap = Maps.newHashMap();
            for (Map.Entry<EventQueue, EventHandler> entry : eventHandlerMap.entrySet()) {
                EventQueue queue = entry.getKey();
                this.eventQueueMap.put(queue.getQueueName(), queue);
            }
        }
    }
}
