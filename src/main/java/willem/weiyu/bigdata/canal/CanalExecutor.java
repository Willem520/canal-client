package willem.weiyu.bigdata.canal;

import java.net.InetSocketAddress;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;

import willem.weiyu.bigdata.bean.BinlogParseResult;
import willem.weiyu.bigdata.bean.EventRow;
import willem.weiyu.bigdata.config.CanalExecutorConfig;
import willem.weiyu.bigdata.constant.EventType;

/**
 * @Author weiyu
 * @Description
 * @Date 2019/3/29 18:20
 */
public class CanalExecutor implements Callable<Void> {
    private static Logger log = LoggerFactory.getLogger(CanalExecutor.class);
    private boolean running = false;
    private CanalExecutorConfig config;
    private CanalConnector canalConnector;

    public CanalExecutor(CanalExecutorConfig canalClientConfig) {
        this.config = canalClientConfig;
        if (config.isCluster()) {
            canalConnector = CanalConnectors.newClusterConnector(config.getZkServers(), config.getDestination(),
                    config.getUsername(), config.getPassword());
        } else {
            canalConnector = CanalConnectors.newSingleConnector(
                    new InetSocketAddress(config.getUrl(), config.getPort()),
                    config.getDestination(), config.getUsername(), config.getPassword());
        }
    }

    @Override
    public Void call() throws Exception {
        start();
        return null;
    }

    public void start() {
        try {
            if (running) {
                log.warn("canal client has already started");
                return;
            }
            running = true;
            canalConnector.connect();
            canalConnector.subscribe();
            while (running) {
                Message message = canalConnector.getWithoutAck(100);
                long batchId = message.getId();
                List<CanalEntry.Entry> entryList = message.getEntries();
                if (batchId == -1 || entryList.isEmpty()) {
                    log.info("******[{}]获取的内容为空******",config.getDestination());
                    Thread.sleep(1000);
                } else {
                    printEntries(entryList);
                    canalConnector.ack(batchId);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error("occur error,message:[{}]", e.getMessage());
        } finally {
            stop();
        }
    }

    public void stop() {
        if (canalConnector != null) {
            canalConnector.disconnect();
        }
        running = false;
    }

    private void printEntries(List<CanalEntry.Entry> entryList) {
        for (CanalEntry.Entry entry : entryList) {
            if (entry.getEntryType() != CanalEntry.EntryType.ROWDATA) {
                continue;
            }
            try {
                CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                if (rowChange != null) {
                    if (rowChange.getIsDdl()) {
                        continue;
                    }
                    BinlogParseResult result = parseEvent(entry.getHeader(), rowChange);
                    //log.info("binlogResult:[{}]", result);
                }
            } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
            }
        }
    }

    private BinlogParseResult parseEvent(CanalEntry.Header header, CanalEntry.RowChange rowChange) {
        List<String> eventRows = new ArrayList<>();

        long timestamp = header.getExecuteTime();
        long now = System.currentTimeMillis();
        long delay = now - timestamp;

        for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
            EventRow eventRow = new EventRow();
            eventRow.setBody(new EventRow.Body());
            eventRow.setHeader(new EventRow.Header());
            eventRow.setTimestamp(now);
            switch (header.getEventType()) {
                case INSERT:
                    eventRow.getHeader().setAction(EventType.INSERT.name());
                    break;
                case UPDATE:
                    eventRow.getHeader().setAction(EventType.UPDATE.name());
                    break;
                case DELETE:
                    eventRow.getHeader().setAction(EventType.DELETE.name());
                    break;
                default:
                    continue;
            }

            eventRow.getHeader().setDatabase(header.getSchemaName());
            eventRow.getHeader().setTable(header.getTableName());
            eventRow.getHeader().setLogfile(header.getLogfileName());
            eventRow.getHeader().setLogOffset(header.getLogfileOffset());
            eventRow.getHeader().setSourceTimestamp(timestamp);
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
                eventRow.getBody().setId(id);
                eventRow.getBody().setOldData(data);
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
                eventRow.getBody().setData(data);
                eventRow.getBody().setChanges(changes);
            }

            eventRows.add(JSONObject.toJSONString(eventRow));
            if (log.isInfoEnabled()) {
                log.info("Receive binlog [{}.{}]-[{}], changes:{}, position:[{}], delay:[{}]ms",
                        eventRow.getHeader().getDatabase(),
                        eventRow.getHeader().getTable(), eventRow.getHeader().getAction(),
                        eventRow.getBody().getChanges(),
                        eventRow.getHeader().getLogfile() + "," + eventRow.getHeader().getLogOffset(), delay);
            }
        }
        return new BinlogParseResult(eventRows);
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
