package net.scat.sync.consumer.impl;

import lombok.AllArgsConstructor;
import net.scat.sync.consumer.base.AbstractSyncConsumer;
import net.scat.sync.consumer.base.SyncReader;
import net.scat.sync.consumer.base.SyncWriter;
import net.scat.sync.mapper.SyncBaseConfigMapper;
import net.scat.sync.mapper.SyncEsWriterConfigMapper;
import net.scat.sync.mapper.SyncSqlReaderConfigMapper;
import net.scat.sync.model.SyncEsWriterConfig;
import net.scat.sync.model.SyncSqlReaderConfig;
import org.springframework.stereotype.Component;

@Component
@AllArgsConstructor
public class SyncEsBySqlConsumer extends AbstractSyncConsumer<SyncSqlReaderConfig, SyncEsWriterConfig> {
    private final SyncEsWriter syncEsWriter;
    private final SyncSqlReader syncSqlReader;
    private final SyncSqlReaderConfigMapper readerConfigMapper;
    private final SyncEsWriterConfigMapper writerConfigMapper;
    @Override
    protected SyncReader<SyncSqlReaderConfig> getReader() {
        return syncSqlReader;
    }

    @Override
    protected SyncWriter<SyncEsWriterConfig> getWriter() {
        return syncEsWriter;
    }

    @Override
    protected SyncBaseConfigMapper<SyncSqlReaderConfig> getReaderConfigMapper() {
        return readerConfigMapper;
    }

    @Override
    protected SyncBaseConfigMapper<SyncEsWriterConfig> getWriterConfigMapper() {
        return writerConfigMapper;
    }
}
