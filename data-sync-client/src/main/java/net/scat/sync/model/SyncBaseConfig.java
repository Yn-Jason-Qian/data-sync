package net.scat.sync.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class SyncBaseConfig {
    private Integer id;
    /**
     * 数据库
     */
    private String db;
    /**
     * 数据表
     */
    private String table;
    /**
     * 更新操作时，需要比较的字段，未设置则必更新
     */
    private String updateCompareFields;
    /**
     * 查询更新，1 是，0 否
     */
    private Integer updateByQuery;
    /**
     * 逻辑删除键名
     */
    private String delKeyName;
    /**
     * 逻辑删除"已删除"对应的值
     */
    private String hasDelVal;
    /**
     * 删除操作会删除关联整条数据，0 否（更新），1 是，主表通常要设为1
     */
    private Integer delWholeData;
    /**
     * 是否主表
     */
    private Integer isMainTable;

}
