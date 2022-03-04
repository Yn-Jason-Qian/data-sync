package net.scat.sync.server.aliyun;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.aliyun.dts.subscribe.clients.exception.TimestampSeekException;
import com.aliyun.dts.subscribe.clients.metastore.AbstractUserMetaStore;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import net.scat.sync.server.SyncResetPointService;
import net.scat.sync.server.config.AliyunDTSProperties;
import net.scat.sync.server.constant.RedisConstants;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.redisson.Redisson;
import org.redisson.api.RLock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.text.ParseException;
import java.util.Date;

/**
 * <h2>阿里云DTS消费位点自定义存储</h2><br/>
 * 自定义存储 > dts服务器存储 > 传入的initial timestamp > 新建Dstore的起始位点
 */
@Component
@Slf4j
public class AliyunDTSUserMetaStore extends AbstractUserMetaStore implements SyncResetPointService {
    private static final String RESET_FLAG = "1";
    @Autowired
    private RedisTemplate<String, String> redisTemplate;
    @Autowired
    private AliyunDTSProperties properties;
    @Autowired
    private Redisson redisson;

    @Override
    protected void saveData(String groupID, String toStoreJson) {
        log.info("Save check point to redis start.");
        RLock lock = redisson.getLock(getLockKey(groupID));
        try {
            lock.lock();
            if (ifReset(groupID)) {
                clearReset(groupID);
                // 与dts源代码耦合，这里抛出TimestampSeekException会使当前的DTSConsumer死掉，
                // 从而AliyunDTSRecordListener中的监视后台线程重新拉起一个新的DTSConsumer
                throw new TimestampSeekException("Check point has been reset.");
            }
            redisTemplate.opsForValue().set(getKey(groupID), toStoreJson);
        } finally {
            lock.unlock();
        }
    }

    @Override
    protected String getData(String groupID) {
        return redisTemplate.opsForValue().get(getKey(groupID));
    }

    private String getKey(String groupId) {
        return RedisConstants.APPLICATION_BASE_SYNC + RedisConstants.ALIYUN_DTS_USER_META_STORE + groupId;
    }

    private String getResetKey(String groupId) {
        return RedisConstants.APPLICATION_BASE_SYNC + RedisConstants.ALIYUN_DTS_USER_META_STORE_RESET + groupId;
    }

    private String getLockKey(String groupId) {
        return RedisConstants.APPLICATION_BASE_SYNC + RedisConstants.ALIYUN_DTS_USER_META_LOCK_RESET + groupId;
    }

    @Override
    public boolean reset(String groupId, long timestamp) {
        long initCheckPoint = Long.parseLong(properties.getGroups().stream()
                .filter(s -> s.getSid().equals(groupId)).findFirst().get().getInitCheckpoint());
        if (timestamp < initCheckPoint) {
            log.error("Timestamp can not be less than initCheckPoint, timestamp={}, initCheckPoint={}", timestamp, initCheckPoint);
            return false;
        }

        String data = getData(groupId);
        if (data == null) {
            log.error("Can not get aliyun dts user metaStore info from redis, groupId=" + groupId);
            return false;
        }
        JSONObject jsonObject = JSON.parseObject(data);
        JSONArray streamCheckpoint = jsonObject.getJSONArray("streamCheckpoint");
        streamCheckpoint.getJSONObject(0).put("timestamp", timestamp);
        jsonObject.put("streamCheckpoint", new JSONArray(Lists.newArrayList(streamCheckpoint)));
        RLock lock = redisson.getLock(getLockKey(groupId));
        try {
            lock.lock();
            redisTemplate.opsForValue().set(getResetKey(groupId), RESET_FLAG);
            redisTemplate.opsForValue().set(getKey(groupId), jsonObject.toJSONString());
        } finally {
            lock.unlock();
        }
        return true;
    }

    @Override
    public boolean reset(String groupId, String formatDate) {
        Date date;
        try {
            date = DateUtils.parseDate(formatDate,"yyyyMMddHHmmss");
        } catch (ParseException e) {
            log.error("Parse date error.", e);
            return false;
        }
        return reset(groupId, date.getTime() / 1000);
    }

    private boolean ifReset(String groupId) {
        String s = redisTemplate.opsForValue().get(getResetKey(groupId));

        return StringUtils.isNotBlank(s) && RESET_FLAG.equals(s);
    }

    private void clearReset(String groupId) {
        redisTemplate.delete(getResetKey(groupId));
    }

}
