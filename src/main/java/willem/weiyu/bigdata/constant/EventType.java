package willem.weiyu.bigdata.constant;

/**
 * @Author weiyu
 * @Description
 * @Date 2019/2/25 16:16
 */
public enum EventType {

    INSERT(0), UPDATE(1), DELETE(2), DUMP(3);

    private int value;

    EventType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

}
