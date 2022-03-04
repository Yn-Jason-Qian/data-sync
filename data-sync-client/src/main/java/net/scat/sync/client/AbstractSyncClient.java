package net.scat.sync.client;

import lombok.extern.slf4j.Slf4j;
import net.scat.sync.consumer.impl.SyncEsBySqlConsumer;
import net.scat.sync.model.DataChangeEvent;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.util.CollectionUtils;

import java.util.*;

@Slf4j
public abstract class AbstractSyncClient implements SyncClient, ApplicationContextAware {
    private Map<String, List<DataChangeEventConsumer>> consumers;
    @Autowired(required = false)
    private SyncEsBySqlConsumer defaultSyncEsConsumer;

    /**
     * 根据[数据库.表名]分发事件
     * @param event
     */
    @Override
    public void receive(DataChangeEvent event) {
        DataChangeEvent.MetaData metaData = event.getMetaData();
        List<DataChangeEventConsumer> consumers = this.consumers.get(getKey(metaData.getDb(), metaData.getTable()));

        boolean handled = false;
        if (!CollectionUtils.isEmpty(consumers)) {
            for (DataChangeEventConsumer consumer : consumers) {
                consumer.consume(event);
            }
            handled = true;
        }
        if (defaultSyncEsConsumer != null && defaultSyncEsConsumer.support(event)) {
            try {
                defaultSyncEsConsumer.consume(event);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            handled = true;
        }
        if (!handled){
            log.warn("Can not find support consumer for event, event=" + event.getSimpleInfo());
        }
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        Map<String, DataChangeEventConsumer> beans = applicationContext.getBeansOfType(DataChangeEventConsumer.class);
        if (CollectionUtils.isEmpty(beans)) {
            log.error("Can not find any DataChangeEventConsumer entity");
            consumers = Collections.emptyMap();
            return;
        }
        Map<String, List<DataChangeEventConsumer>> mappings = new HashMap<>();
        for (DataChangeEventConsumer consumer : beans.values()) {
            Map<String, List<String>> dbAndTables = consumer.supportDBAndTables();
            if (CollectionUtils.isEmpty(dbAndTables)) {
                log.error("Support dbs and tables can not be empty, consumer=" + consumer.getClass().getSimpleName());
                continue;
            }

            for (Map.Entry<String, List<String>> entry : dbAndTables.entrySet()) {
                String db = entry.getKey();
                List<String> tables = entry.getValue();
                if (StringUtils.isBlank(db) || CollectionUtils.isEmpty(tables)) {
                    log.error("Support db can not be blank, and tables can not be empty, consumer=" + consumer.getClass().getSimpleName());
                    continue;
                }
                for (String table : tables) {
                    mappings.computeIfAbsent(getKey(db, table), s -> new ArrayList<>()).add(consumer);
                }
            }
        }
        consumers = mappings;
    }

    private String getKey(String db, String table) {
        return db + "." + table;
    }
}
