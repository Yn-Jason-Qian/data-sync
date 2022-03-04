package net.scat.sync.consumer.impl;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.scat.sync.consumer.base.SyncWriter;
import net.scat.sync.model.DataChangeEvent;
import net.scat.sync.model.SyncEsWriterConfig;
import net.scat.sync.utils.DateUtils;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.script.Script;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;

@Component
@Slf4j
@AllArgsConstructor
public class SyncEsWriter implements SyncWriter<SyncEsWriterConfig> {
    private final RestHighLevelClient esClient;

    @Override
    public void upsert(SyncEsWriterConfig config, List<Map<String, Object>> data) throws Exception {
        BulkRequest bulk = new BulkRequest();
        for (Map<String, Object> datum : data) {
            String id = getId(config.getEsIdPrefix(), String.valueOf(datum.get(config.getEsIdName())));
            UpdateRequest request = new UpdateRequest(config.getEsIndex(), config.getEsType(), id)
                    .doc(buildSource(datum)).docAsUpsert(true)
                    .retryOnConflict(5);
            if (StringUtils.isNotBlank(config.getEsRouting())) {
                request.routing(config.getEsRouting());
            }
            bulk.add(request);
        }
        BulkResponse response = esClient.bulk(bulk, RequestOptions.DEFAULT);
        log.info("Upsert es result: " + JSON.toJSONString(response));
    }

    @Override
    public void update(SyncEsWriterConfig config, Map<String, Object> updateData) throws IOException {
        Object foreignKeyValue = updateData.get(config.getEsForeignKeyName());
        if (foreignKeyValue == null) {
            log.warn("Foreign key value can not be null, foreign key name={}, updateData={}",
                    config.getEsForeignKeyName(), updateData);
            return;
        }
        UpdateByQueryRequest request = new UpdateByQueryRequest(config.getEsIndex())
                .setDocTypes(config.getEsType())
                .setRefresh(true)
                .setAbortOnVersionConflict(false)
                .setQuery(QueryBuilders.boolQuery().must(QueryBuilders.termQuery(config.getEsForeignKeyName(), foreignKeyValue)))
                .setScript(buildScript(config, updateData));
        esClient.updateByQuery(request, RequestOptions.DEFAULT);
    }

    private Script buildScript(SyncEsWriterConfig config, Map<String, Object> updateData) {
        String script = config.getUpdateScript();
        for (Map.Entry<String, Object> entry : updateData.entrySet()) {
            script = script.replaceAll("%"+ entry.getKey() +"%", convertEsValue(entry.getValue()));
        }
        return Script.parse(Settings.builder().put("source", script).build());
    }

    private String convertEsValue(Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof String) {
            return "'"+ value +"'";
        }
        if (value instanceof Date) {
            return "'" + DateUtils.format((Date) value) + "'";
        }
        return value.toString();
    }

    @Override
    public void delete(SyncEsWriterConfig config, DataChangeEvent event) throws IOException {
        if (config.getIsMainTable() == 1) {
            Object id = tryGetValueFromEvent(config.getIdOriginName(), event);
            if (id == null) {
                return;
            }
            DeleteRequest request = new DeleteRequest(config.getEsIndex(), config.getEsType(), getId(config.getEsIdPrefix(), String.valueOf(id)));
            if (StringUtils.isNotBlank(config.getEsRouting())) {
                request.routing(config.getEsRouting());
            }
            esClient.delete(request, RequestOptions.DEFAULT);
        } else {
            if (StringUtils.isBlank(config.getEsForeignKeyName())) {
                return;
            }
            Object value = tryGetValueFromEvent(config.getForeignKeyOriginName(), event);
            if (value == null) {
                return;
            }
            DeleteByQueryRequest request = new DeleteByQueryRequest(config.getEsIndex()).setDocTypes(config.getEsType());
            request.setQuery(QueryBuilders.termQuery(config.getEsForeignKeyName(), convertEsValue(value)));
            esClient.deleteByQuery(request, RequestOptions.DEFAULT);
        }
    }

    private Object tryGetValueFromEvent(String fieldName, DataChangeEvent event) {
        if (StringUtils.isBlank(fieldName)) {
            return null;
        }
        Object value = null;
        if (fieldName.equals(event.getPrimaryKeyData().getName())) {
            value = event.getPrimaryKeyData().getValue();
        } else {
            if (event.getBefore() != null && !CollectionUtils.isEmpty(event.getBefore().getFields())) {
                value = event.getBefore().getFields().get(fieldName);
            } else if (event.getAfter() != null && !CollectionUtils.isEmpty(event.getAfter().getFields())) {
                value = event.getAfter().getFields().get(fieldName);
            }
        }
        return value;
    }

    private XContentBuilder buildSource(Map<String, Object> data) throws Exception {
        XContentBuilder source = XContentFactory.jsonBuilder();
        source.startObject();
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            addFieldWithValueType(source, entry);
        }
        source.endObject();
        return source;
    }

    private void addFieldWithValueType(XContentBuilder source, Map.Entry<String, Object> entry) throws IOException {
        Object value = entry.getValue();
        String name = entry.getKey();
        if (value == null) {
            source.nullField(name);
        } else if (value instanceof Long) {
            source.field(name, (Long) value);
        } else if (value instanceof Integer) {
            source.field(name, (Integer) value);
        } else if (value instanceof Iterable) {
            source.field(name, (Iterable<?>) value);
        } else if (value instanceof Float) {
            source.field(name, (Float) value);
        } else if (value instanceof String) {
            source.field(name, value.toString());
        } else if (value instanceof Date) {
            source.field(name, DateUtils.format((Date) value));
        }else {
            source.field(name, value);
        }
    }

    private String getId(String idPrefix, String id) {
        if (StringUtils.isBlank(idPrefix)) {
            return id;
        }
        return idPrefix + id;
    }
}
