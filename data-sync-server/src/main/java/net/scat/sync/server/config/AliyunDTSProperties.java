package net.scat.sync.server.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Data
@Component
@ConfigurationProperties(prefix = "aliyun.dts")
public class AliyunDTSProperties {

    private List<Group> groups = new ArrayList<>();

    @Data
    public static class Group {
        // kafka broker url
        private String brokerUrl;
        // topic to consume, partition is 0
        private String topic;
        // user password and sid for auth
        private String sid;
        private String userName;
        private String password;
        // initial checkpoint for first seek(a timestamp to set, eg 1566180200 if you want (Mon Aug 19 10:03:21 CST 2019))
        private String initCheckpoint;
        private boolean isForceUseCheckpoint = false;
    }
}
