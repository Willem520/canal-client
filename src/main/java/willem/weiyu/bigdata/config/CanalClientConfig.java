package willem.weiyu.bigdata.config;

import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import lombok.Data;

/**
 * @Author weiyu
 * @Description
 * @Date 2019/2/19 16:59
 */
@Configuration
@ConfigurationProperties(prefix = "canal.client")
@Data
public class CanalClientConfig {

    private List<CanalExecutorConfig> configs;
}
