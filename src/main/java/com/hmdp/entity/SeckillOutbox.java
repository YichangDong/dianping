package com.hmdp.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

import java.io.Serializable;
import java.time.LocalDateTime;

@Data
@EqualsAndHashCode(callSuper = false)
@Accessors(chain = true)
@TableName("tb_seckill_outbox")
public class SeckillOutbox implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 消息ID，此处直接使用订单ID作为主键
     */
    @TableId(value = "id", type = IdType.INPUT)
    private Long id;

    private Long userId;

    private Long voucherId;

    /**
     * 状态：0-INIT，1-SUCCESS，2-FAIL
     */
    private Integer status;

    private LocalDateTime createTime;

    private LocalDateTime updateTime;

}

