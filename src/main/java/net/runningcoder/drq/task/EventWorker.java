package net.runningcoder.drq.task;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.WorkHandler;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import net.runningcoder.drq.Event;
import net.runningcoder.drq.EventPublishThread;
import net.runningcoder.drq.queue.EventQueue;
import net.runningcoder.drq.task.handler.EventHandler;

import java.util.concurrent.Executors;

/**
 * Created by machine on 2017/10/22.
 */
@Slf4j
@Data
public class EventWorker {
    /**
     * 缓存大小
     */
    private int bufferSize = 1024;
    /**
     * 工作线程大小
     */
    private int workerSize = 64;
    /**
     * 队列
     */
    private EventQueue eventQueue;
    /**
     * 处理器
     */
    private EventHandler eventHandler;
    /**
     * 并发组件
     */
    private Disruptor<Event> disruptor;
    /**
     * 发布线程
     */
    private EventPublishThread thread;

    public EventWorker(EventQueue eventQueue, int bufferSize, int workerSize) {
        this.eventQueue = eventQueue;
        this.bufferSize = bufferSize;
        this.workerSize = workerSize;
        init();
    }

    /**
     * 初始化
     */
    public void init() {
        // 创建Disruptor
        disruptor = new Disruptor<>(
                Event::new,
                bufferSize,
                Executors.defaultThreadFactory(),
                ProducerType.SINGLE,
                new BlockingWaitStrategy()
        );

        // 处理异常
        disruptor.setDefaultExceptionHandler(new ExceptionHandler<Event>() {
            public void handleEventException(Throwable throwable, long l, Event event) {
                log.error("handleEventException", throwable);
            }

            public void handleOnStartException(Throwable throwable) {
                log.error("handleOnStartException", throwable);
            }

            public void handleOnShutdownException(Throwable throwable) {
                log.error("handleOnShutdownException", throwable);
            }
        });

        // 创建工作者处理器
        WorkHandler<Event> workHandler = event -> {
            Object obj = event.getObj();
            try {
                eventHandler.onEvent(obj);
                //处理成功
                eventQueue.success(obj);
            } catch (Exception e) {
                eventQueue.fail(obj);
            }
        };

        // 创建工作者处理器
        WorkHandler[] workHandlers = new WorkHandler[workerSize];
        for (int i = 0; i < workerSize; i++) {
            workHandlers[i] = workHandler;
        }

        // 告知 Disruptor 由工作者处理器处理
        disruptor.handleEventsWithWorkerPool(workHandlers);

        // 初始化线程
        thread = new EventPublishThread(eventQueue, disruptor);
    }

    public void start() {
        if (eventHandler == null) {
            throw new IllegalStateException("please set eventHandler before run start()...");
        }
        // 启动 Disruptor
        disruptor.start();

        // 启动发布者线程
        thread.start();
    }

    public void stop() {
        // 停止发布者线程
        thread.shutdown();
        // 停止 Disruptor
        disruptor.shutdown();
    }
}
