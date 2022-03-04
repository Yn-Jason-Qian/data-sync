package net.scat.sync.model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class SyncSqlReaderConfig extends SyncBaseConfig {
    /**
     * 查询整条数据sql，参数用#{field_name}封装
     */
    private String queryWholeSql;
    /**
     * 查询更新数据sql，主表无需设置，注意查询结果为单一数据，参数用#{field_name}封装
     */
    private String queryUpdateSql;
    /**
     * 删除操作时，查询整条数据的sql，参数用#{field_name}封装
     */
    private String queryDeleteSql;
}
