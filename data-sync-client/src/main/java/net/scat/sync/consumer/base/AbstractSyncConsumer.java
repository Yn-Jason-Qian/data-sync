package net.scat.sync.consumer.base;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import net.scat.sync.enums.DataChangeEventType;
import net.scat.sync.mapper.SyncBaseConfigMapper;
import net.scat.sync.model.DataChangeEvent;
import net.scat.sync.model.SyncBaseConfig;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public abstract class AbstractSyncConsumer<R extends SyncBaseConfig, W extends SyncBaseConfig> implements InitializingBean {
    protected volatile Map<String, List<R>> readerConfigMap = Collections.emptyMap();
    protected volatile Map<String, List<W>> writerConfigMap = Collections.emptyMap();
    private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread thread = new Thread(r, this.getClass().getSimpleName() + "_config_refresher");
        thread.setDaemon(true);
        return thread;
    });

    public void consume(DataChangeEvent event) throws Exception{
        DataChangeEvent.MetaData metaData = event.getMetaData();
        // 获取库表相关的同步配置
        List<R> readerConfig = readerConfigMap.get(getKey(metaData.getDb(), metaData.getTable()));
        if (CollectionUtils.isEmpty(readerConfig)) {
            log.warn("No sync reader config set for this event, event=" + event.getSimpleInfo());
            return;
        }
        // 获取库表相关的同步配置
        List<W> writerConfigs = writerConfigMap.get(getKey(metaData.getDb(), metaData.getTable()));
        if (CollectionUtils.isEmpty(readerConfig)) {
            log.warn("No sync writer config set for this event, event=" + event.getSimpleInfo());
            return;
        }
        for (R config : readerConfig) {
            Optional<W> writerConfig = writerConfigs.stream().filter(s -> s.getId().equals(config.getId())).findFirst();
            if (!writerConfig.isPresent()) {
                log.warn("Reader config can not match any writerConfig, readerConfig id = " + config.getId());
                continue;
            }
            consumeOne(event, config, writerConfig.get());
        }
    }

    private void consumeOne(DataChangeEvent event, R readerConfig, W writerConfig) throws Exception {
        if (event.getEventType() == DataChangeEventType.INSERT) {
            handleInsert(event, readerConfig, writerConfig);
        } else if (event.getEventType() == DataChangeEventType.UPDATE) {
            handleUpdate(event, readerConfig, writerConfig);
        } else {
            handleDelete(event, readerConfig, writerConfig);
        }
    }

    private void handleInsert(DataChangeEvent event, R readerConfig, W writerConfig) throws Exception {
        // 新增操作，读取整条数据做插入更新（不存在则插入，存在则更新）
        if (readerConfig.getIsMainTable() == 1) {
            upsertAll(event, readerConfig, writerConfig);
        } else {
            upsertByPage(event, readerConfig, writerConfig);
        }
    }

    private void handleUpdate(DataChangeEvent event, R readerConfig, W writerConfig) throws Exception {
        // 更新操作，比较需要保留的字段，是否有变更
        if (!compareFields(Lists.newArrayList(readerConfig.getUpdateCompareFields().split(",")),
                event.getBefore(), event.getAfter())) {
            return;
        }
        // 如果支持逻辑删除，并且会导致整条数据的删除，则跳转到删除操作
        if (StringUtils.isNotBlank(readerConfig.getDelKeyName()) &&
                hasDel(event, readerConfig.getDelKeyName(), readerConfig.getHasDelVal()) &&
                readerConfig.getDelWholeData() == 1) {
            handleDelete(event, readerConfig, writerConfig);
            return;
        }
        Object primaryKey = event.getPrimaryKeyData().getValue();
        // 主表有变更，则读取全部字段，做整体更新
        if (readerConfig.getIsMainTable() == 1) {
            upsertAll(event, readerConfig, writerConfig);
            return;
        }
        // 从表变更，且非删除操作时（删除操作不适用局部变更）
        if (readerConfig.getUpdateByQuery() == 1 && event.getEventType() != DataChangeEventType.DELETE) {
            // 关联数据变更都相同时，做局部变更
            Map<String, Object> updateData = getReader().getUpdateData(readerConfig, primaryKey, event);
            if (CollectionUtils.isEmpty(updateData)) {
                return;
            }
            getWriter().update(writerConfig, updateData);
        } else {
            upsertByPage(event, readerConfig, writerConfig);
        }
    }

    private void upsertAll(DataChangeEvent event, R readerConfig, W writerConfig) throws Exception {
        List<Map<String, Object>> data = getReader().getWholeData(readerConfig, event.getPrimaryKeyData().getValue(), event);
        if (CollectionUtils.isEmpty(data)) {
            return;
        }
        getWriter().upsert(writerConfig, data);
    }

    private void upsertByPage(DataChangeEvent event, R readerConfig, W writerConfig) throws Exception {
        Object primaryKey = event.getPrimaryKeyData().getValue();
        // 关联数据变更不同时，则做整体更新
        int count = getReader().countWholeData(readerConfig, primaryKey, event);
        if (count == 0) {
            return;
        }
        int start = 0;
        int limit = 50;
        // 因为从表关联数据量可能会很大，这里做批次更新，防止一次更新的数据量过大
        while (start < count) {
            List<Map<String, Object>> data = getReader().getPageOfWholeData(readerConfig, primaryKey, event, start, limit);
            if (CollectionUtils.isEmpty(data)) {
                break;
            }
            getWriter().upsert(writerConfig, data);
            start += data.size();
        }
    }

    private boolean hasDel(DataChangeEvent data, String deleteKeyName, String hasDelValue) {
        if (data.getEventType() == DataChangeEventType.DELETE) {
            return true;
        }
        DataChangeEvent.FieldData field = data.getAfter().getFields().get(deleteKeyName);
        if (field == null) {
            return false;
        }
        Object deleteKey = field.getValue();
        if (deleteKey == null) {
            return false;
        }
        return Objects.equals(String.valueOf(deleteKey), hasDelValue);
    }

    private void handleDelete(DataChangeEvent event, R readerConfig,  W writerConfig) throws Exception {
        if (writerConfig.getDelWholeData() == 1) {
            getWriter().delete(writerConfig, event);
        } else {
            handleUpdate(event, readerConfig, writerConfig);
        }
    }

    public boolean support(DataChangeEvent event) {
        String key = getKey(event.getMetaData().getDb(), event.getMetaData().getTable());
        return readerConfigMap.containsKey(key) && writerConfigMap.containsKey(key);
    }

    protected abstract SyncReader<R> getReader();

    protected abstract SyncWriter<W> getWriter();

    /**
     * 比较用来判断数据是否更新的字段值
     * @param updateCompareFieldNames 字段名称
     * @param before 更新前数据
     * @param after 更新后数据
     * @return 有更新 true，无更新 false
     */
    private boolean compareFields(List<String> updateCompareFieldNames, DataChangeEvent.Entity before,
                                  DataChangeEvent.Entity after) {
        if (CollectionUtils.isEmpty(updateCompareFieldNames)) {
            return true;
        }
        if (before == null && after == null) {
            return false;
        }
        if (before == null || after == null) {
            return true;
        }
        Map<String, DataChangeEvent.FieldData> beforeFields = before.getFields();
        Map<String, DataChangeEvent.FieldData> afterFields = after.getFields();
        for (String fieldName : updateCompareFieldNames) {
            fieldName = fieldName.trim();
            DataChangeEvent.FieldData beforeField = beforeFields.get(fieldName);
            DataChangeEvent.FieldData afterField = afterFields.get(fieldName);
            if (beforeField == null && afterField == null) {
                continue;
            }
            if (beforeField == null || afterField == null) {
                return true;
            }
            if (!Objects.equals(beforeField.getValue(), afterField.getValue())) {
                return true;
            }
        }
        return false;
    }

    private String getKey(String db, String table) {
        return db + "." + table;
    }

    @Override
    public void afterPropertiesSet() {
        refreshConfigMap();
        executorService.scheduleAtFixedRate(this::refreshConfigMap, 60, 60, TimeUnit.SECONDS);
    }

    protected abstract SyncBaseConfigMapper<R> getReaderConfigMapper();

    protected abstract SyncBaseConfigMapper<W> getWriterConfigMapper();

    private void refreshConfigMap() {
        List<R> readerConfig;
        try {
            readerConfig = getReaderConfigMapper().selectAll();
        } catch (Exception e) {
            log.error(Thread.currentThread().getName() + ": Get sync reader config error.", e);
            return;
        }
        if (CollectionUtils.isEmpty(readerConfig)) {
            log.warn(Thread.currentThread().getName() + ": No sync reader config been set.");
            return;
        }
        Map<String, List<R>> map = new HashMap<>();
        for (R config : readerConfig) {
            map.computeIfAbsent(getKey(config.getDb(), config.getTable()), s -> new ArrayList<>())
                    .add(config);
        }
        this.readerConfigMap = map;

        List<W> writerConfigs;
        try {
            writerConfigs = getWriterConfigMapper().selectAll();
        } catch (Exception e) {
            log.error(Thread.currentThread().getName() + ": Get sync writer config error.", e);
            return;
        }
        if (CollectionUtils.isEmpty(writerConfigs)) {
            log.warn(Thread.currentThread().getName() + ": No sync writer config been set.");
            return;
        }
        Map<String, List<W>> temp = new HashMap<>();
        for (W config : writerConfigs) {
            temp.computeIfAbsent(getKey(config.getDb(), config.getTable()), s -> new ArrayList<>())
                    .add(config);
        }
        this.writerConfigMap = temp;
    }
}
