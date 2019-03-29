package willem.weiyu.bigdata.canal;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import willem.weiyu.bigdata.config.CanalClientConfig;
import willem.weiyu.bigdata.config.CanalExecutorConfig;

/**
 * @Author weiyu
 * @Description
 * @Date 2019/2/25 16:16
 */
@Component
public class CustomizedCanalConnector implements CommandLineRunner {
    private static Logger log = LoggerFactory.getLogger(CustomizedCanalConnector.class);

    @Autowired
    private CanalClientConfig canalClientConfig;

    private ExecutorService executorPool;

    @Override
    public void run(String... args) throws Exception {
        init();
    }

    private void init() throws Exception {
        log.info("******begin init******");
        Optional.ofNullable(canalClientConfig).orElseThrow(() -> new Exception("canalClientConfig is null"));
        List<CanalExecutorConfig> canalExecutorConfigs =
                Optional.ofNullable(canalClientConfig.getConfigs()).orElse(Collections.emptyList());
        if (canalExecutorConfigs.isEmpty()) {
            throw new Exception("canalExecutorConfigs is empty");
        }
        executorPool = Executors.newFixedThreadPool(canalExecutorConfigs.size());

        executorPool.invokeAll(canalExecutorConfigs.stream().map(conf->new CanalExecutor(conf)).collect(Collectors.toList()));
    }

    @PreDestroy
    public void stop(){
        executorPool.shutdown();
    }
}
