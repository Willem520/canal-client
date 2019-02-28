package willem.weiyu.bigdata.bean;

import lombok.*;

import java.util.Map;
import java.util.Set;

/**
 * @Author weiyu
 * @Description
 * @Date 2019/2/25 16:16
 */
@Getter
@Setter
@ToString
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class EventRow {

    private long timestamp;

    private Header header;

    private Body body;

    @Getter
    @Setter
    @ToString
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Header {

        private String database;

        private String table;

        private String action;

        private long sourceTimestamp;

        private String logfile;

        private long logOffset;
    }

    @Getter
    @Setter
    @ToString
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Body {

        private String id;

        private Map<String, Object> oldData;

        private Map<String, Object> data;

        private Set<String> changes;
    }

}
