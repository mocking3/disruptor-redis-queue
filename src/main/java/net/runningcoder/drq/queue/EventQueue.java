package net.runningcoder.drq.queue;

import com.google.common.collect.Lists;
import lombok.Data;
import net.runningcoder.drq.utils.LocalIpUtils;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by machine on 2017/10/22.
 */
@Data
public class EventQueue {
    public static final long DEFAULT_AWAIT_IN_MILLIS = 500;

    private RedisTemplate<String, String> queueRedis;

    private String queueName;

    private String processingQueueName;
    private int processingErrorRetryCount;

    private String failedQueueName;

    private String bakQueueName;
    private long maxBakSize;

    private long awaitInMillis = DEFAULT_AWAIT_IN_MILLIS;

    private Lock lock = new ReentrantLock();
    private Condition notEmpty = lock.newCondition();

    public EventQueue(String queueName,
                      RedisTemplate<String, String> queueRedis,
                      int processingErrorRetryCount,
                      long maxBakSize) {
        this.queueName = queueName;
        this.queueRedis = queueRedis;
        this.processingQueueName = queueName + "_processing_queue_" + LocalIpUtils.getLocalHostIP();
        this.processingErrorRetryCount = processingErrorRetryCount;
        this.failedQueueName = queueName + "_failed_queue";
        this.bakQueueName = queueName + "_bak_queue";
        this.maxBakSize = maxBakSize;
    }

    public String next() {
        while (true) {
            String id = null;
            try {
                //2. 从等待 Queue POP，然后 PUSH 到本地处理队列
                id = queueRedis.opsForList().rightPopAndLeftPush(queueName, processingQueueName);
            } catch (Exception e) {
                //3. 发生了网络异常后警告，然后人工或定期检测
                //将本地任务队列长时间未消费的任务推送回等待队列
                continue;
            }

            //4. 返回获取的任务
            if (id != null) {
                awaitInMillis = DEFAULT_AWAIT_IN_MILLIS;
                return id;
            }
            lock.lock();
            try {
                //
                if (awaitInMillis < 1000) {
                    awaitInMillis = awaitInMillis + awaitInMillis;
                }
                notEmpty.await(awaitInMillis, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                //ignore
            } finally {
                lock.unlock();
            }
        }
    }

    public void success(String id) {
        queueRedis.opsForList().remove(processingQueueName, 0, id);
        String failCountKey = getFailCountName(id);
        if (queueRedis.opsForValue().get(failCountKey) != null)
            queueRedis.delete(failCountKey);
    }

    public void fail(final String id) {
        String failCountKey = getFailCountName(id);

        final int failedCount = queueRedis.opsForValue().increment(failCountKey, 1).intValue();
        //如果小于等于重试次数，则直接添加到等待队列队尾
        if (failedCount <= processingErrorRetryCount) {
            queueRedis.opsForList().leftPush(queueName, id);
            queueRedis.opsForList().remove(processingQueueName, 0, id);
        } else {
            queueRedis.opsForList().leftPush(failedQueueName, id);
            queueRedis.opsForList().remove(processingQueueName, 0, id);
            queueRedis.delete(failCountKey);
        }
    }

    //接收其他系统推送的任务
    public void enqueueToBack(String id) {
        queueRedis.opsForList().leftPush(queueName, id);
        queueRedis.opsForList().leftPush(bakQueueName, id);
        if (queueRedis.opsForList().size(bakQueueName) > getMaxBakSize()) {
            queueRedis.opsForList().rightPop(bakQueueName);
        }
    }

    private static String getFailCountName(String id) {
        return new StringBuffer("failed:").append(id).append(":count").toString();
    }
}
