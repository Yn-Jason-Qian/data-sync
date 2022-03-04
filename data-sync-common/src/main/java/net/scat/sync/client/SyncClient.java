package net.scat.sync.client;


import net.scat.sync.model.DataChangeEvent;

public interface SyncClient {

    void receive(DataChangeEvent event);
}
