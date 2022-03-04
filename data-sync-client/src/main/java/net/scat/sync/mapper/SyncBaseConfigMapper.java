package net.scat.sync.mapper;


import net.scat.sync.model.SyncBaseConfig;

import java.util.List;

public interface SyncBaseConfigMapper<T extends SyncBaseConfig>{
    List<T> selectAll();
}
