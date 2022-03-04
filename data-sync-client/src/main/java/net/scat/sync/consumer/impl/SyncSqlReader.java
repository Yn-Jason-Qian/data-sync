package net.scat.sync.consumer.impl;

import com.google.common.collect.ImmutableMap;
import lombok.extern.slf4j.Slf4j;
import net.scat.sync.consumer.base.SyncReader;
import net.scat.sync.enums.DataChangeEventType;
import net.scat.sync.model.DataChangeEvent;
import net.scat.sync.model.SyncSqlReaderConfig;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.jdbc.core.ColumnMapRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;

@Component
@Slf4j
public class SyncSqlReader implements ApplicationContextAware, SyncReader<SyncSqlReaderConfig> {
    private static Map<String, JdbcTemplate> dbToTemplateMap;

    @Override
    public List<Map<String, Object>> getWholeData(SyncSqlReaderConfig config, Object primaryKey, DataChangeEvent event) {
        JdbcTemplate jdbcTemplate = dbToTemplateMap.get(config.getDb());
        return jdbcTemplate.query(getQueryWholeSql(config, event), new ColumnMapRowMapper());
    }

    @Override
    public Map<String, Object> getUpdateData(SyncSqlReaderConfig config, Object primaryKey, DataChangeEvent event) {
        JdbcTemplate jdbcTemplate = dbToTemplateMap.get(config.getDb());
        List<Map<String, Object>> resultList = jdbcTemplate.query(
                buildSql(config.getQueryUpdateSql(), event.getAfter().getFields()), new ColumnMapRowMapper());
        if (CollectionUtils.isEmpty(resultList)) {
            return Collections.emptyMap();
        }
        return resultList.get(0);
    }

    @Override
    public Integer countWholeData(SyncSqlReaderConfig config, Object primaryKey, DataChangeEvent event) {
        JdbcTemplate jdbcTemplate = dbToTemplateMap.get(config.getDb());
        return jdbcTemplate.queryForObject(getCountSql(getQueryWholeSql(config, event)), Integer.class);
    }

    @Override
    public List<Map<String, Object>> getPageOfWholeData(SyncSqlReaderConfig config, Object primaryKey, DataChangeEvent event, int start, int limit) {
        JdbcTemplate jdbcTemplate = dbToTemplateMap.get(config.getDb());
        return jdbcTemplate.query(getPageSql(getQueryWholeSql(config, event), start, limit), new ColumnMapRowMapper());
    }

    private String getQueryWholeSql(SyncSqlReaderConfig config, DataChangeEvent event) {
        String sql = config.getQueryWholeSql();
        if (event.getEventType() == DataChangeEventType.DELETE) {
            sql = config.getQueryDeleteSql();
        }

        Map<String, DataChangeEvent.FieldData> params;
        if (event.getEventType() == DataChangeEventType.DELETE) {
            params = event.getBefore().getFields();
        } else {
            params = event.getAfter().getFields();
        }
        return buildSql(sql, params);
    }

    private String buildSql(String sql, Map<String, DataChangeEvent.FieldData> params) {
        if (StringUtils.isBlank(sql) || CollectionUtils.isEmpty(params)) {
            return sql;
        }
        for (Map.Entry<String, DataChangeEvent.FieldData> entry : params.entrySet()) {
            sql = sql.replaceAll("#\\{"+ entry.getKey() +"}", getSqlValue(entry.getValue().getValue()));
        }
        return sql;
    }

    private String getSqlValue(Object value) {
        return String.valueOf(value);
    }

    private String getCountSql(String sql) {
        return "select count(*) from (" + sql + ") as a";
    }

    private String getPageSql(String sql, int start, int limit) {
        return "select * from ("+ sql +") as a limit "+ start +"," + limit;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        if (dbToTemplateMap != null) {
            return;
        }
        Map<String, JdbcTemplate> templates = applicationContext.getBeansOfType(JdbcTemplate.class);
        if (CollectionUtils.isEmpty(templates)) {
            log.error("There are`t any jdbcTemplate has been init.");
            return;
        }

        ImmutableMap.Builder<String, JdbcTemplate> builder = new ImmutableMap.Builder<>();
        builder.put("mall_goods", templates.get("mallJdbcTemplate"));
        builder.put("mall_health", templates.get("mallJdbcTemplate"));
        builder.put("mall_order", templates.get("mallJdbcTemplate"));
        builder.put("wbnetmeeting", templates.get("emktJdbcTemplate"));
        builder.put("wbcms", templates.get("primaryJdbcTemplate"));
        dbToTemplateMap = builder.build();
    }
}
