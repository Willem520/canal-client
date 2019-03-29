package willem.weiyu.bigdata.config;

import lombok.Data;

/**
 * @Author weiyu
 * @Date 2019/3/29 17:58
 */
@Data
public class CanalExecutorConfig {
    private boolean isCluster;

    private String zkServers;

    private String url;

    private Integer port;

    private String destination;

    private String username;

    private String password;

    private String bootstrapServer;

    private String topic;
}
