package net.scat.sync.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class SyncEsWriterConfig extends SyncBaseConfig {
    /**
     * es更新script，参数用%field_name%封装，无需对字符类型设置''
     */
    private String updateScript;

    /**
     * es索引名
     */
    private String esIndex;

    /**
     * type，默认doc
     */
    private String esType;

    /**
     * es路由键
     */
    private String esRouting;

    /**
     * es主键id名称，对应sql查询字段
     */
    private String esIdName;

    /**
     * 主键在表中的名称，删除操作时用于获取主键值
     */
    private String idOriginName;

    /**
     * es id字段的值前缀
     */
    private String esIdPrefix;

    /**
     * 从表更新时，关联的外键名称
     */
    private String esForeignKeyName;
    /**
     * 关联键在表中的字段名，用于删除操作获取关联值'
     */
    private String foreignKeyOriginName;
}
