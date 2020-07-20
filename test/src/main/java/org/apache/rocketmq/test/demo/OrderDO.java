package org.apache.rocketmq.test.demo;

/**
 * @Auther: Lenovo
 * @Date: 2020/4/27 17:27
 * @Description:
 */

public class OrderDO {

    private String orderId;

    private String desc;

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }
}
