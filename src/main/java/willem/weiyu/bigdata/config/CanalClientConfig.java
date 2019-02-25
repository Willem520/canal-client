package willem.weiyu.bigdata.config;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

/**
 * @Author weiyu
 * @Description
 * @Date 2019/2/19 16:59
 */
@Configuration
@Data
public class CanalClientConfig {
    @Value("${canal.client.isCluster}")
    private boolean isCluster;

    @Value("${canal.client.zkServers}")
    private String zkServers;

    @Value("${canal.client.url}")
    private String url;

    @Value("${canal.client.port}")
    private Integer port;

     @Value("${canal.client.destination}")
    private String destination;

    @Value("${canal.client.username}")
    private String username;

    @Value("${canal.client.password}")
    private String password;
}
