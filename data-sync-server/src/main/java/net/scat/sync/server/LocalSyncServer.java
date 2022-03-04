package net.scat.sync.server;

import net.scat.sync.client.SyncClient;
import net.scat.sync.model.DataChangeEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
public class LocalSyncServer implements SyncServer{
    @Autowired
//    @Qualifier("localSyncClient")
    @Qualifier("localMultiSyncClient")
    private SyncClient client;
    @Override
    public void send(DataChangeEvent event) {
        client.receive(event);
    }
}
