package willem.weiyu.bigdata;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * @Author weiyu
 * @Description
 * @Date 2019/2/19 16:55
 */
@SpringBootApplication(scanBasePackages = "willem.weiyu.bigdata")
public class CanalClientApplication {

    public static void main(String[] args) {
        SpringApplication.run(CanalClientApplication.class, args);
    }
}
