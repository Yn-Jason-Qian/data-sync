package net.scat.sync.server;


import net.scat.sync.model.DataChangeEvent;

public interface SyncServer {

    void send(DataChangeEvent event);
}
