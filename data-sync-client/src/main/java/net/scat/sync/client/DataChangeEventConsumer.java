package net.scat.sync.client;


import net.scat.sync.model.DataChangeEvent;

import java.util.List;
import java.util.Map;

public interface DataChangeEventConsumer {

    void consume(DataChangeEvent event);

    Map<String, List<String>> supportDBAndTables();
}
