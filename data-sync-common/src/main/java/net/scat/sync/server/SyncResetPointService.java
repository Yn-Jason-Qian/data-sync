package net.scat.sync.server;

public interface SyncResetPointService {
    boolean reset(String groupId, long timestamp);

    boolean reset(String groupId, String formatDate);
}
