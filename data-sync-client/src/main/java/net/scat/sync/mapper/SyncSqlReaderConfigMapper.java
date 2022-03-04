package net.scat.sync.mapper;

import net.scat.sync.model.SyncSqlReaderConfig;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface SyncSqlReaderConfigMapper extends SyncBaseConfigMapper<SyncSqlReaderConfig> {
    @Select("select sbc.*, rc.base_conf_id,rc.query_whole_sql,rc.query_update_sql,rc.query_delete_sql \n" +
            "from sync_base_config sbc \n" +
            "inner join sync_sql_reader_config rc on rc.base_conf_id = sbc.id  \n" +
            "where sbc.is_del = 0")
    List<SyncSqlReaderConfig> selectAll();

}
