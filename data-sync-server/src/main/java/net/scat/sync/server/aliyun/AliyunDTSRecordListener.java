package net.scat.sync.server.aliyun;

import com.aliyun.dts.subscribe.clients.ConsumerContext;
import com.aliyun.dts.subscribe.clients.DTSConsumer;
import com.aliyun.dts.subscribe.clients.DefaultDTSConsumer;
import com.aliyun.dts.subscribe.clients.common.RecordListener;
import com.aliyun.dts.subscribe.clients.record.*;
import com.aliyun.dts.subscribe.clients.record.value.Value;
import com.aliyun.dts.subscribe.clients.record.value.ValueType;
import com.google.common.collect.ImmutableMap;
import lombok.extern.slf4j.Slf4j;
import net.scat.sync.enums.DataChangeEventType;
import net.scat.sync.enums.DataFieldType;
import net.scat.sync.model.DataChangeEvent;
import net.scat.sync.server.SyncServer;
import net.scat.sync.server.config.AliyunDTSProperties;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Component
@Slf4j
public class AliyunDTSRecordListener implements RecordListener {
    private final EnumSet<OperationType> types = EnumSet.of(OperationType.DELETE, OperationType.UPDATE, OperationType.INSERT);
//    @Qualifier("localSyncServer")
    @Qualifier("rocketMqSyncServer")
    @Autowired
    private SyncServer server;
    @Autowired
    private AliyunDTSProperties properties;
    @Autowired
    private AliyunDTSUserMetaStore store;
    private final List<ConsumerWrapper> consumers = new ArrayList<>();
    private final ScheduledExecutorService checkerExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread thread = new Thread(r, "AliyunDTSRecordListener_consumer_checker");
        thread.setDaemon(true);
        return thread;
    });

    @PostConstruct
    public void init() {
        List<AliyunDTSProperties.Group> groups = properties.getGroups();
        if (CollectionUtils.isEmpty(groups)) {
            log.error("Aliyun DTS Properties has not been set. Please check application yaml file.");
            return;
        }
        for (AliyunDTSProperties.Group group : groups) {
            ConsumerWrapper consumer = new ConsumerWrapper(group);
            consumers.add(consumer);
            consumer.start();
        }
    }

    @PreDestroy
    public void destroy() {
        checkerExecutor.shutdownNow();
        for (ConsumerWrapper consumer : consumers) {
            consumer.exit();
        }
    }

    private class ConsumerWrapper {
        private final AliyunDTSProperties.Group groupProperties;
        private volatile boolean workerDown = false;
        private ConsumerContext consumerContext;
        private Thread worker;
        private volatile boolean exited = false;

        ConsumerWrapper(AliyunDTSProperties.Group group) {
            this.groupProperties = group;
        }

        private void start() {
            startWorker();
            startChecker();
        }

        private void startWorker() {
            worker = new Thread(() -> {
                workerDown = false;
                ConsumerContext.ConsumerSubscribeMode subscribeMode = ConsumerContext.ConsumerSubscribeMode.SUBSCRIBE;
                ConsumerContext consumerContext = new ConsumerContext(groupProperties.getBrokerUrl(),
                        groupProperties.getTopic(),
                        groupProperties.getSid(),
                        groupProperties.getUserName(),
                        groupProperties.getPassword(),
                        groupProperties.getInitCheckpoint(), subscribeMode);
                // 使用自定义位点存储
                consumerContext.setUserRegisteredStore(store);
                DTSConsumer dtsConsumer = new DefaultDTSConsumer(consumerContext);
                dtsConsumer.addRecordListeners(Collections.singletonMap("", AliyunDTSRecordListener.this));
                synchronized (this) {
                    if (this.exited) {
                        return;
                    }
                    this.consumerContext = consumerContext;
                }
                // 启动后会阻塞，直到服务退出
                dtsConsumer.start();
                //服务异常会退出阻塞，使线程运行结束
                workerDown();
            }, "AliyunDTS_consumer_" + groupProperties.getSid());
            worker.start();
        }

        private void workerDown() {
            this.workerDown = true;
            consumerContext.exit();
        }

        private synchronized void exit() {
            this.exited = true;
            consumerContext.exit();
        }

        private void startChecker() {
            checkerExecutor.scheduleAtFixedRate(this::checkAndRestart, 10, 10, TimeUnit.SECONDS);
        }

        private void checkAndRestart() {
            if (exited) {
                return;
            }
            if (!this.workerDown || this.worker.isAlive()) {
                return;
            }
            // 消费服务停了，重新启动
            log.warn("Worker has been down, restart now.");
            startWorker();
        }
    }

    @Override
    public void consume(DefaultUserRecord record) {
        if (!types.contains(record.getOperationType())) {
            record.commit("");
            return;
        }
        try {
            DataChangeEvent event = new DataChangeEvent();

            // 提取库名，表名
            RecordSchema schema = record.getSchema();
            DataChangeEvent.MetaData metaData = new DataChangeEvent.MetaData();
            metaData.setDb(schema.getDatabaseName().orElse(""));
            metaData.setTable(schema.getTableName().orElse(""));
            // 提取主键名
            if (schema.getPrimaryIndexInfo() != null && CollectionUtils.isNotEmpty(schema.getPrimaryIndexInfo().getIndexFields())) {
                metaData.setPrimaryKeyName(schema.getPrimaryIndexInfo().getIndexFields()
                        .stream().map(RecordField::getFieldName).collect(Collectors.joining("_")));
            }
            event.setMetaData(metaData);
            // 获取主键，操作时间，事件类型
            event.setTimestamp(new Date(record.getSourceTimestamp() * 1000));
            event.setEventType(DataChangeEventType.valueOf(record.getOperationType().toString()));
            DataChangeEvent.FieldData primaryKeyData = null;
            // 提取变更前数据
            if (record.getBeforeImage() != null) {
                DataChangeEvent.Entity before = convertImage(record.getBeforeImage(), schema);
                event.setBefore(before);
                primaryKeyData = before.getFields().get(metaData.getPrimaryKeyName());

            }
            // 提取变更后数据
            if (record.getAfterImage() != null) {
                DataChangeEvent.Entity after = convertImage(record.getAfterImage(), schema);
                event.setAfter(after);
                primaryKeyData = after.getFields().get(metaData.getPrimaryKeyName());

            }
            if (primaryKeyData != null) {
                event.setPrimaryKey(primaryKeyData.getValue());
                event.setPrimaryKeyType(primaryKeyData.getType());
            }
            event.setPrimaryKeyData(primaryKeyData);
            log.info("consume event,{}",event.getSimpleInfo());
            server.send(event);
            record.commit("");
        } catch (Exception e) {
            log.error(String.format("Aliyun DTS consume record error, record[offset=%s,timestamp=%s,]",
                    record.getOffset(), record.getSourceTimestamp()), e);
        }
    }

    private DataChangeEvent.Entity convertImage(RowImage rowImage, RecordSchema schema) {
        DataChangeEvent.Entity entity = new DataChangeEvent.Entity();
        for (RecordField field : schema.getFields()) {
            String fieldName = field.getFieldName();
            Value<?> value = rowImage.getValue(fieldName);

            DataChangeEvent.FieldData fieldData = new DataChangeEvent.FieldData();
            fieldData.setName(fieldName);
            fieldData.setValue(getData(value));
            fieldData.setPrimaryKey(field.isPrimary());
            fieldData.setType(getType(value));
            entity.getFields().put(fieldName, fieldData);
        }
        return entity;
    }

    private Object getData(Value<?> value) {
        if (value == null || value.getData() == null) {
            return null;
        }
        Object data = value.getData();
        ValueType type = value.getType();
        if (type == ValueType.STRING) {
            return new String(((ByteBuffer)data).array());
        } else if (type == ValueType.INTEGER_NUMERIC || type == ValueType.DECIMAL_NUMERIC) {
            return data;
        } else if (type == ValueType.DATETIME || type == ValueType.UNIX_TIMESTAMP) {
            try {
                return DateUtils.parseDate(data.toString(), "yyyy-MM-dd HH:mm:ss.SSS", "yyyy-MM-dd", "yyyy-MM-dd HH:mm:ss");
            } catch (ParseException e) {
                log.error("", e);
                return data;
            }
        } else if (type == ValueType.FLOAT_NUMERIC) {
            return data;
        }
        log.warn("type={}, class={}, data={}", type, data.getClass(), data);
        return data;
    }

    private static final Map<ValueType, DataFieldType> mappings;
    static {
        ImmutableMap.Builder<ValueType, DataFieldType> builder = new ImmutableMap.Builder<>();
        builder.put(ValueType.INTEGER_NUMERIC, DataFieldType.INTEGER);
        builder.put(ValueType.FLOAT_NUMERIC, DataFieldType.FLOAT);
        builder.put(ValueType.DECIMAL_NUMERIC, DataFieldType.DECIMAL);
        builder.put(ValueType.STRING, DataFieldType.STRING);
        builder.put(ValueType.DATETIME, DataFieldType.DATE);
        builder.put(ValueType.UNIX_TIMESTAMP, DataFieldType.DATE);
        mappings = builder.build();
    }
    private DataFieldType getType(Value<?> value) {
        if (value == null || value.getType() == null) {
            return DataFieldType.OTHER;
        }
        DataFieldType type = mappings.get(value.getType());
        if (type == null) {
            return DataFieldType.OTHER;
        }
        return type;
    }
}
