package net.scat.sync.consumer.base;


import net.scat.sync.model.DataChangeEvent;
import net.scat.sync.model.SyncBaseConfig;

import java.util.List;
import java.util.Map;

public interface SyncReader<T extends SyncBaseConfig> {
    /**
     * 获取整条数据
     */
    List<Map<String, Object>> getWholeData(T config, Object primaryKey, DataChangeEvent event);

    /**
     * 获取更新数据
     */
    Map<String, Object> getUpdateData(T config, Object primaryKey, DataChangeEvent event);

    /**
     * 获取整体数据数量
     */
    Integer countWholeData(T config, Object primaryKey, DataChangeEvent event);

    /**
     * 获取整体数据分页
     */
    List<Map<String, Object>> getPageOfWholeData(T config, Object primaryKey, DataChangeEvent event, int start, int limit);
}
