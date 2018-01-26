package net.runningcoder.drqdemo;

import lombok.extern.slf4j.Slf4j;
import net.runningcoder.drq.queue.EventQueue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Created by wangmaocheng on 2017/10/25.
 */
@Slf4j
@EnableScheduling
@Component
public class Producer {

    private int i;

    @Autowired
    private EventQueue<String> eventQueue;

    @Scheduled(fixedDelay = 5000)
    public void send() {
        String str = "num-" + (++i);
        eventQueue.enqueueToBack(str);
        log.info("开始生产-{}", str);
    }

}
