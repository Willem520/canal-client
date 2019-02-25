package willem.weiyu.bigdata.canal;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import willem.weiyu.bigdata.bean.BinlogParseResult;
import willem.weiyu.bigdata.bean.EpRow;
import willem.weiyu.bigdata.config.CanalClientConfig;
import willem.weiyu.bigdata.constant.EventType;

import java.net.InetSocketAddress;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

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

    private volatile boolean running = false;

    private CanalConnector canalConnector;

    @Override
    public void run(String... args) throws Exception {
        init();
        start();
    }

    private void init() throws Exception {
        log.info("******begin init");
        Optional.ofNullable(canalClientConfig).orElseThrow(()->new Exception("canalClientConfig is null"));
        boolean isCluster = canalClientConfig.isCluster();
        if (isCluster){
            canalConnector = CanalConnectors.newClusterConnector(canalClientConfig.getZkServers(),canalClientConfig.getDestination(),
                    canalClientConfig.getUsername(),canalClientConfig.getPassword());
        } else {
            canalConnector = CanalConnectors.newSingleConnector(
                    new InetSocketAddress(canalClientConfig.getUrl(), canalClientConfig.getPort()),
                    canalClientConfig.getDestination(),canalClientConfig.getUsername(),canalClientConfig.getPassword());
        }
    }

    public synchronized void start(){
        try{
            if (running){
                log.warn("canal client has already started");
                return;
            }
            running = true;
            canalConnector.connect();
            canalConnector.subscribe();
            while (running){
                Message message = canalConnector.getWithoutAck(100);
                long batchId = message.getId();
                List<CanalEntry.Entry> entryList = message.getEntries();
                if (batchId == -1 || entryList.isEmpty()) {
                    log.info("******获取的内容为空******");
                } else {
                    printEntries(entryList);
                    canalConnector.ack(batchId);
                }
            }
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            stop();
        }
    }

    public void stop(){
        if (canalConnector != null){
            canalConnector.disconnect();
        }
        running = false;
    }

    private void printEntries(List<CanalEntry.Entry> entryList){
        for (CanalEntry.Entry entry : entryList) {
            try {
                CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                if (rowChange != null) {
                    if (rowChange.getIsDdl()) {
                        continue;
                    }
                    BinlogParseResult result = parseEvent(entry.getHeader(), rowChange);
                    log.info("binlogResult:[{}]",result);
                }
            } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
                continue;
            }
        }
    }

    private BinlogParseResult parseEvent(CanalEntry.Header header, CanalEntry.RowChange rowChange) {
        List<String> epRows = new ArrayList<>();

        long timestamp = header.getExecuteTime();
        long now = System.currentTimeMillis();
        long delay = now - timestamp;

        for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
            EpRow epRow = new EpRow();
            epRow.setBody(new EpRow.Body());
            epRow.setHeader(new EpRow.Header());
            epRow.setTimestamp(now);
            switch (header.getEventType()) {
                case INSERT:
                    epRow.getHeader().setAction(EventType.INSERT.name());
                    break;
                case UPDATE:
                    epRow.getHeader().setAction(EventType.UPDATE.name());
                    break;
                case DELETE:
                    epRow.getHeader().setAction(EventType.DELETE.name());
                    break;
                default:
                    continue;
            }

            epRow.getHeader().setDatabase(header.getSchemaName());
            epRow.getHeader().setTable(header.getTableName());
            epRow.getHeader().setLogfile(header.getLogfileName());
            epRow.getHeader().setLogOffset(header.getLogfileOffset());
            epRow.getHeader().setSourceTimestamp(timestamp);
            List<String> idList = new ArrayList<>();
            List<CanalEntry.Column> beforeColumnsList = rowData.getBeforeColumnsList();
            if (beforeColumnsList != null && !beforeColumnsList.isEmpty()) {
                Map<String, Object> data = new HashMap<>(beforeColumnsList.size());
                for (CanalEntry.Column column : beforeColumnsList) {
                    data.put(column.getName(), getValue(column));

                    if (column.getIsKey()) {
                        idList.add(getStringValue(column));
                    }
                }
                String id = StringUtils.join(idList, "_");
                epRow.getBody().setId(id);
                epRow.getBody().setOldData(data);
            }
            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
            if (afterColumnsList != null && !afterColumnsList.isEmpty()) {
                Set<String> changes = new HashSet<>();
                Map<String, Object> data = new HashMap<>(afterColumnsList.size());
                for (CanalEntry.Column column : afterColumnsList) {
                    data.put(column.getName(), getValue(column));
                    if (column.getUpdated()) {
                        changes.add(column.getName());
                    }
                    if (column.getIsKey()) {
                        idList.add(getStringValue(column));
                    }
                }
                epRow.getBody().setData(data);
                epRow.getBody().setChanges(changes);
            }

            epRows.add(JSONObject.toJSONString(epRow));
            if (log.isInfoEnabled()) {
                log.info("Receive binlog epRow[{}.{}]-[{}], changes:{}, position:[{}], delay:[{}]ms", epRow.getHeader().getDatabase(),
                        epRow.getHeader().getTable(), epRow.getHeader().getAction(), epRow.getBody().getChanges(), epRow.getHeader().getLogfile()+","+epRow.getHeader().getLogOffset(), delay);
            }
        }
        return new BinlogParseResult(epRows);
    }

    private Object getValue(CanalEntry.Column column) {
        int sqlType = column.getSqlType();
        String stringValue = column.getValue();
        if (stringValue == null || stringValue.isEmpty()) {
            return stringValue;
        }
        Object value;
        try {
            switch (sqlType) {
                case Types.TINYINT:
                case Types.SMALLINT:
                case Types.INTEGER:
                    value = Integer.parseInt(stringValue);
                    break;
                case Types.BIGINT:
                    value = Long.parseLong(stringValue);
                    break;
                case Types.FLOAT:
                case Types.REAL:
                    value = Float.parseFloat(stringValue);
                    break;
                case Types.DOUBLE:
                case Types.DECIMAL:
                    value = Double.parseDouble(stringValue);
                    break;
                case Types.TIMESTAMP:

                    value = LocalDateTime.parse(stringValue, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                    break;
                case Types.DATE:
                    value = LocalDate.parse(stringValue, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
                    break;
                case Types.TIME:
                    value = LocalTime.parse(stringValue, DateTimeFormatter.ofPattern("HH:mm:ss"));
                    break;
                default:
                    value = stringValue;
            }
        } catch (Exception e) {
            log.warn(String.format("%s format fail, value is %s, type is %s, message is %s", column.getName(),
                    stringValue, sqlType, e.getMessage()));
            value = stringValue;
        }
        return value;
    }
    private String getStringValue(CanalEntry.Column column) {
        return column.getValue();
    }

}
