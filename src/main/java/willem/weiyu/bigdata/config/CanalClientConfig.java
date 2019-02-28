package willem.weiyu.bigdata.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * @Author weiyu
 * @Description
 * @Date 2019/2/19 16:59
 */
@Configuration
@EnableConfigurationProperties(CanalClientConfig.class)
@ConfigurationProperties(prefix = "canal.client")
@Data
public class CanalClientConfig {
    private boolean isCluster;

    private String zkServers;

    private String url;

    private Integer port;

    private String destination;

    private String username;

    private String password;
}
