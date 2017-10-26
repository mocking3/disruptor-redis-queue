package net.runningcoder.drqdemo;

import lombok.extern.slf4j.Slf4j;
import net.runningcoder.drq.task.EventWorker;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

/**
 * Created by wangmaocheng on 2017/10/25.
 */
@Slf4j
@Component
public class Consumer implements CommandLineRunner {
    @Autowired
    private EventWorker eventWorker;

    @Override
    public void run(String... strings) throws Exception {
        eventWorker.setEventHandler(obj -> {
            log.info("开始处理-{}", obj.toString());
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        eventWorker.start();
    }
}
