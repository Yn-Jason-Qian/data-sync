package net.scat.sync.model;

import com.alibaba.fastjson.JSON;
import lombok.*;
import lombok.experimental.Accessors;
import net.scat.sync.enums.DataChangeEventType;
import net.scat.sync.enums.DataFieldType;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@Data
public class DataChangeEvent {
    private MetaData metaData;

    private Date timestamp;

    private DataChangeEventType eventType;

    private Entity before;

    private Entity after;

    @Deprecated
    private Object primaryKey;

    @Deprecated
    private DataFieldType primaryKeyType;

    private FieldData primaryKeyData;

    @Data
    public static class MetaData {
        private String db;

        private String table;

        private String primaryKeyName;
    }

    @Data
    public static class Entity {
        private Map<String, FieldData> fields = new HashMap<>();
    }

    @Data
    @Accessors(chain = true)
    public static class FieldData {
        private String name;

        private Object value;

        private boolean isPrimaryKey;

        private DataFieldType type;
    }

    @Getter(AccessLevel.NONE)
    @Setter(AccessLevel.NONE)
    private String simpleInfo;

    public String getSimpleInfo() {
        if (simpleInfo != null) {
            return simpleInfo;
        }
        Map<String, Object> info = new HashMap<>();
        info.put("metaData", metaData);
        info.put("timestamp", timestamp);
        info.put("primaryKey", primaryKey);
        info.put("eventType", eventType);
        simpleInfo = JSON.toJSONString(info);
        return simpleInfo;
    }
}
