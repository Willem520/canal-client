package willem.weiyu.bigdata.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import willem.weiyu.bigdata.config.CanalClientConfig;

import javax.annotation.PostConstruct;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;

/**
 * @Author weiyu005@ke.com
 * @Description
 * @Date 2019/2/19 16:51
 */
@Component
public class SimpleConnector {
    @Autowired
    private CanalClientConfig canalClientConfig;

    @PostConstruct
    public void init(){
        start();
    }

    public void start(){
        Optional.ofNullable(canalClientConfig).orElseThrow(()->new RuntimeException("canalClientConfig is null"));
        CanalConnector connector = CanalConnectors.newSingleConnector(
                new InetSocketAddress(canalClientConfig.getUrl(), canalClientConfig.getPort()),
                canalClientConfig.getDestination(),canalClientConfig.getUsername(),canalClientConfig.getPassword());
        connector.connect();
        connector.subscribe();
        while (true){
            Message message = connector.getWithoutAck(100);
            long batchId = message.getId();
            if (batchId == -1 || message.getEntries().isEmpty()) {
                System.out.println("******获取的内容为空******");
            } else {
                printEntries(message.getEntries());
                connector.ack(batchId);
            }
        }
    }

    private void printEntries(List<CanalEntry.Entry> entryList){
        for (CanalEntry.Entry entry : entryList) {
            try {
                CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                    System.out.println(rowData.toString());
                }
            } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
                continue;
            }
        }
    }
}
