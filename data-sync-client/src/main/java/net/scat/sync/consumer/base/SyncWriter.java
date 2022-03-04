package net.scat.sync.consumer.base;


import net.scat.sync.model.DataChangeEvent;
import net.scat.sync.model.SyncBaseConfig;

import java.util.List;
import java.util.Map;

public interface SyncWriter<T extends SyncBaseConfig> {
    /**
     * 整条数据更新，不存在则插入
     */
    void upsert(T config, List<Map<String, Object>> data) throws Exception;

    /**
     * 局部数据更新
     */
    void update(T config, Map<String, Object> updateData) throws Exception;

    void delete(T config, DataChangeEvent event) throws Exception;
}
