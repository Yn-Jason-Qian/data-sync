package net.scat.sync.mapper;

import net.scat.sync.model.SyncEsWriterConfig;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface SyncEsWriterConfigMapper extends SyncBaseConfigMapper<SyncEsWriterConfig> {
    @Select("select sbc.*, wc.base_conf_id,wc.update_script,wc.es_index,wc.es_type\n" +
            ",wc.es_routing,wc.es_id_name,wc.id_origin_name,wc.es_id_prefix\n" +
            ",wc.es_foreign_key_name,wc.foreign_key_origin_name \n" +
            "from sync_base_config sbc \n" +
            "inner join sync_es_writer_config wc on wc.base_conf_id = sbc.id  \n" +
            "where sbc.is_del = 0")
    List<SyncEsWriterConfig> selectAll();
}
