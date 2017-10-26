package net.runningcoder.drq.queue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import net.runningcoder.drq.utils.LocalIpUtils;
import org.apache.commons.codec.digest.DigestUtils;
import org.springframework.data.redis.core.RedisTemplate;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by machine on 2017/10/22.
 */
@Data
public class EventQueue<T> {
    public static final long DEFAULT_AWAIT_IN_MILLIS = 500;

    private RedisTemplate<Object, Object> queueRedis;

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
                      RedisTemplate<Object, Object> queueRedis,
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

    public T next() {
        while (true) {
            T obj = null;
            try {
                // 从等待 Queue POP，然后 PUSH 到本地处理队列
                obj = (T) queueRedis.opsForList().rightPopAndLeftPush(queueName, processingQueueName);
            } catch (Exception e) {
                // 发生了网络异常后警告，然后人工或定期检测
                // 将本地任务队列长时间未消费的任务推送回等待队列
                continue;
            }

            //4. 返回获取的任务
            if (obj != null) {
                return obj;
            }
            lock.lock();
            try {
                notEmpty.await(awaitInMillis, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                //ignore
            } finally {
                lock.unlock();
            }
        }
    }

    public void success(T obj) {
        queueRedis.opsForList().remove(processingQueueName, 0, obj);
        String failCountKey = getFailCountName(obj);
        if (queueRedis.opsForValue().get(failCountKey) != null)
            queueRedis.delete(failCountKey);
    }

    public void fail(T obj) {
        String failCountKey = getFailCountName(obj);

        final int failedCount = queueRedis.opsForValue().increment(failCountKey, 1).intValue();
        //如果小于等于重试次数，则直接添加到等待队列队尾
        if (failedCount <= processingErrorRetryCount) {
            queueRedis.opsForList().leftPush(queueName, obj);
            queueRedis.opsForList().remove(processingQueueName, 0, obj);
        } else {
            queueRedis.opsForList().leftPush(failedQueueName, obj);
            queueRedis.opsForList().remove(processingQueueName, 0, obj);
            queueRedis.delete(failCountKey);
        }
    }

    //接收其他系统推送的任务
    public void enqueueToBack(T obj) {
        queueRedis.opsForList().leftPush(queueName, obj);
        queueRedis.opsForList().leftPush(bakQueueName, obj);
        if (queueRedis.opsForList().size(bakQueueName) > getMaxBakSize()) {
            queueRedis.opsForList().rightPop(bakQueueName);
        }
    }

    private String getFailCountName(T obj) {
        String md5 = null;
        try {
            md5 = DigestUtils.md5Hex(new ObjectMapper().writeValueAsString(obj));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return new StringBuffer("failed:").append(md5).append(":count").toString();
    }
}
