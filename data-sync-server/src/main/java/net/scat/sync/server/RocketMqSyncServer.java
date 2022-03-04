package net.scat.sync.server;

import lombok.extern.slf4j.Slf4j;
import net.scat.sync.model.DataChangeEvent;
import net.scat.sync.server.constant.RocketMqConstant;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.concurrent.LinkedBlockingQueue;

@Component
@Slf4j
public class RocketMqSyncServer implements SyncServer {
    @Autowired
    private RocketMQTemplate rocketMQTemplate;
    private final LinkedBlockingQueue<DataChangeEvent> failedQueue = new LinkedBlockingQueue<>(10000);
    private volatile boolean closed = false;

    // 重试异常消息发送
    @PostConstruct
    public void initFailCheck() {
        new Thread(() -> {
            while (!closed) {
                try {
                    DataChangeEvent event = failedQueue.take();
                    SendResult sendResult = syncSend(event);

                    if (sendResult == null || sendResult.getSendStatus() != SendStatus.SEND_OK) {
                        log.error("ReSend dataChangeEvent failed, event=" + event.getSimpleInfo());
                        // todo 发送失败报警
                    }
                } catch (InterruptedException e) {
                    log.error("", e);
                }
            }
            // server关闭，记录遗留未发送的事件
            if (failedQueue.size() > 0) {
                DataChangeEvent lostEvent;
                while ((lostEvent = failedQueue.poll()) != null) {
                    log.error("RocketMqSyncServer has been closed, some event will be lost, event=" + lostEvent.getSimpleInfo());
                }
            }
        }, "RocketMqSyncServer_ReSendThread").start();
    }

    private SendResult syncSend(DataChangeEvent event) {
        int retry = 3;
        SendResult sendResult = null;
        while (retry-- > 0) {
            try {
                sendResult = rocketMQTemplate.syncSendOrderly(RocketMqConstant.TOPIC_SYNC, event, getHashKey(event));
                if (sendResult.getSendStatus() == SendStatus.SEND_OK) {
                    break;
                }
            } catch (Exception e) {
                sendResult = null;
                log.error(String.format("ReSend dataChangeEvent error, retry=%s, event=%s", retry, event.getSimpleInfo()), e);
            }
        }
        return sendResult;
    }

    @PreDestroy
    public void close() {
        this.closed = true;
    }

    @Override
    public void send(DataChangeEvent event) {
        Assert.isTrue(!closed, "RocketMqSyncServer has been closed, can not send current event=" + event.getSimpleInfo());
        try {
            rocketMQTemplate.asyncSendOrderly(RocketMqConstant.TOPIC_SYNC, event, getHashKey(event), new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    // do noting
                }
                // 发送异常，存入本地队列，后续重试发送
                @Override
                public void onException(Throwable throwable) {
                    log.error("Async send dataChangeEvent error by rocketMq, event=" + event.getSimpleInfo(), throwable);
                    if (!failedQueue.offer(event)) {
                        log.error("Add failed dataChangeEvent to local queue failed, event=" + event.getSimpleInfo());
                    }
                }
            });
        } catch (Exception e) {
            log.error("Async send dataChangeEvent error by rocketMq, event=" + event.getSimpleInfo(), e);
            throw new RuntimeException("Async send dataChangeEvent error by rocketMq, event=" + event.getSimpleInfo());
        }
    }

    private String getHashKey(DataChangeEvent event) {
        DataChangeEvent.MetaData metaData = event.getMetaData();
        return metaData.getDb() + metaData.getTable() + event.getPrimaryKey();
    }

}
