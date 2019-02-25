package willem.weiyu.bigdata.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

/**
 * @Author weiyu
 * @Description
 * @Date 2019/2/25 16:16
 */
@Data
@AllArgsConstructor
public class BinlogParseResult {

    private List<String> epRowMsgList;

    //private String sourceName;
}
