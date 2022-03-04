package net.scat.sync.client;

import lombok.extern.slf4j.Slf4j;
import net.scat.sync.model.DataChangeEvent;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * 本地消息处理，启用n个队列，对应n个线程，队列与线程一一对应，保证本地顺序性
 */
@Component
@Slf4j
public class LocalMultiSyncClient extends AbstractSyncClient implements SyncClient {
    private List<Worker> workers;
    private final static int WORKER_SIZE = 16;
    private final static int QUEUE_SIZE = 1000;
    private volatile boolean closed = false;

    @PostConstruct
    public void init() {
        workers = new ArrayList<>(WORKER_SIZE);
        for (int i = 0; i < WORKER_SIZE; i++) {
            Worker worker = new Worker("LocalMultiSyncClient_worker_" + i);
            worker.start();
            workers.add(worker);
        }
    }

    @PreDestroy
    public void closed() {
        closed = true;
    }

    @Override
    public void receive(DataChangeEvent event) {
        Assert.isTrue(!closed, "LocalMultiSyncClient has been closed, event=" + event.getSimpleInfo());
        Assert.notNull(event, "Event");
        DataChangeEvent.MetaData metaData = event.getMetaData();
        Assert.isTrue(metaData != null, "Event metaData is null, event=" +  event.getSimpleInfo());

        int hash = hash((metaData.getDb() + metaData.getTable() + event.getPrimaryKey()));
        int index = hash & (WORKER_SIZE - 1);
        Worker worker = workers.get(index);
        boolean success;
        try {
            success = worker.queue.offer(event, 1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.error("", e);
            success = false;
        }
        if (!success) {
            throw new RuntimeException(String.format("LocalMultiSyncClient offer event to local queue failed, worker=%s, queueSize=%s, event=%s",
                    worker.getName(), worker.queue.size(), event.getSimpleInfo()));
        }
    }

    private static int hash(Object key) {
        int h;
        return (key == null) ? 0 : (h = key.hashCode()) ^ (h >>> 16);
    }

    private class Worker extends Thread{
        private final LinkedBlockingQueue<DataChangeEvent> queue = new LinkedBlockingQueue<>(QUEUE_SIZE);
        Worker(String name){
            setName(name);
        }

        @Override
        public void run() {
            while (!closed || queue.size() > 0) {
                DataChangeEvent event;
                try {
                    event = queue.take();
                } catch (InterruptedException e) {
                    log.error("", e);
                    continue;
                }
                try {
                    log.info(getName() + ": consumer event=" + event.getSimpleInfo());
                    LocalMultiSyncClient.super.receive(event);
                } catch (Exception e) {
                    log.error("Event consumer error, event=" + event.getSimpleInfo(), e);
                }
            }
        }

    }
}
