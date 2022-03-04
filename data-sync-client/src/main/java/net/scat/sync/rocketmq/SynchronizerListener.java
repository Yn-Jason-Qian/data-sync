package net.scat.sync.rocketmq;

import lombok.extern.slf4j.Slf4j;
import net.scat.sync.client.SyncClient;
import net.scat.sync.enums.DataFieldType;
import net.scat.sync.model.DataChangeEvent;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.spring.annotation.ConsumeMode;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.apache.rocketmq.spring.core.RocketMQPushConsumerLifecycleListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.ParseException;
import java.time.LocalTime;
import java.util.Date;

@Component
@Slf4j
@RocketMQMessageListener(topic = "",
        consumerGroup = "",
        consumeMode = ConsumeMode.ORDERLY)
public class SynchronizerListener implements RocketMQListener<DataChangeEvent>, RocketMQPushConsumerLifecycleListener {
    @Autowired
    @Qualifier("localMultiSyncClient")
    private SyncClient client;
    @Override
    public void onMessage(DataChangeEvent message) {
        boolean success = false;
        log.info("消费MQ:{}", message.getSimpleInfo());
        int retry = 3;
        while (retry-- > 0 && !success) {
            try {
                DataChangeEvent event = adjustEventData(message);
                client.receive(event);
                success = true;
            } catch (Exception e) {
                log.error("消费MQ异常:{}", message.getSimpleInfo());
                log.error("消费MQ异常:"+e);
            }
        }
        // 处理失败，消息重试
        if (!success) {
            throw new RuntimeException("Consume message failed, message=" + message.getSimpleInfo());
        }
    }

    /**
     * 设置顺序消费最大重试次数，不设置会无限重试消费
     * https://help.aliyun.com/document_detail/43490.html
     * @param consumer 当前的consumer实体
     */
    @Override
    public void prepareStart(DefaultMQPushConsumer consumer) {
        consumer.setMaxReconsumeTimes(16);
    }

    /**
     * 校准event中的value值
     * 因为经过json序列化反序列化，date,integer,double,decimal的类型不一致，这里统一处理成相同类型
     * @param event
     * @return
     */
    private DataChangeEvent adjustEventData(DataChangeEvent event) {
        if (event == null) {
            return null;
        }
        if (event.getPrimaryKey() != null && event.getPrimaryKeyType() != null) {
            DataChangeEvent.FieldData fieldData = new DataChangeEvent.FieldData();
            fieldData.setValue(event.getPrimaryKey());
            fieldData.setType(event.getPrimaryKeyType());
            adjustFieldData(fieldData);
            event.setPrimaryKey(fieldData.getValue());
        }
        if (event.getBefore() != null && !CollectionUtils.isEmpty(event.getBefore().getFields())){
            event.getBefore().getFields().values().forEach(this::adjustFieldData);
        }
        if (event.getAfter() != null && !CollectionUtils.isEmpty(event.getAfter().getFields())) {
            event.getAfter().getFields().values().forEach(this::adjustFieldData);
        }
        return event;
    }

    private void adjustFieldData(DataChangeEvent.FieldData fieldData) {
        if (fieldData == null || fieldData.getValue() == null || fieldData.getType() == null) {
            return;
        }
        DataFieldType type = fieldData.getType();
        Object value = fieldData.getValue();
        Class<?> valueClazz = value.getClass();
        if (valueClazz.equals(type.getClazz())) {
            return;
        }
        if (type == DataFieldType.INTEGER && (Number.class.isAssignableFrom(valueClazz)
                || valueClazz.equals(String.class))) {
            fieldData.setValue(new BigInteger(String.valueOf(value)));
            return;
        }
        if (type == DataFieldType.FLOAT && (Number.class.isAssignableFrom(valueClazz)
                || valueClazz.equals(String.class))) {
            fieldData.setValue(new Double(String.valueOf(value)));
            return;
        }
        if (type == DataFieldType.DECIMAL && (Number.class.isAssignableFrom(valueClazz)
                || valueClazz.equals(String.class))) {
            fieldData.setValue(new BigDecimal(String.valueOf(value)));
            return;
        }
        if (type == DataFieldType.DATE) {
            if (valueClazz.equals(Long.class)) {
                fieldData.setValue(new Date((Long) value));
                return;
            }
            if (valueClazz.equals(String.class)) {
                if (((String) value).matches("([0-1][0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])")) {
                    try {
                        // 时分秒 格式，转换为 LocalTime
                        fieldData.setValue(LocalTime.parse((String) value));
                    } catch (Exception e) {
                        log.error("adjustFieldData LocalTime.parse value:{}", value, e);
                    }
                    return;
                }
                try {
                    fieldData.setValue(DateUtils.parseDate((String) value,
                            "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm:ss.SSS", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"));
                    return;
                } catch (ParseException e) {
                    log.error("adjustFieldData DateUtils.parseDate value:{}", value, e);
                }
            }
        }
    }
}
