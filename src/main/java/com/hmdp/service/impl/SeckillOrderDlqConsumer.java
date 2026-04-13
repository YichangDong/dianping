package com.hmdp.service.impl;

import java.io.IOException;
import java.util.Collections;

import jakarta.annotation.Resource;

import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import com.hmdp.dto.VoucherOrderMessage;
import com.hmdp.service.ISeckillOutboxService;
import com.hmdp.utils.MqConstants;
import com.rabbitmq.client.Channel;

import lombok.extern.slf4j.Slf4j;

/**
 * 死信队列消费者，处理最终消费失败的消息并回滚Redis状态
 */
@Slf4j
@Component
public class SeckillOrderDlqConsumer {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private ISeckillOutboxService seckillOutboxService;

    private static final DefaultRedisScript<Long> SECKILL_ROLLBACK_SCRIPT;

    static {
        SECKILL_ROLLBACK_SCRIPT = new DefaultRedisScript<>();
        SECKILL_ROLLBACK_SCRIPT.setLocation(new ClassPathResource("seckill_rollback.lua"));
        SECKILL_ROLLBACK_SCRIPT.setResultType(Long.class);
    }

    @RabbitListener(queues = MqConstants.SECKILL_ORDER_DLQ)
    public void handleDlqMessage(VoucherOrderMessage message, Channel channel, @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {
        long orderId = message.getOrderId();
        long userId = message.getUserId();
        long voucherId = message.getVoucherId();

        log.error("死信队列收到最终失败的秒杀订单消息, 准备触发回滚操作, orderId={}", orderId);

        try {
            // 1. 执行Redis库存和下单标记回滚
            stringRedisTemplate.execute(
                    SECKILL_ROLLBACK_SCRIPT,
                    Collections.emptyList(),
                    String.valueOf(voucherId),
                    String.valueOf(userId)
            );

            // 2. 标记本地消息表为最终失败 (2-FAIL)
            seckillOutboxService.update().set("status", 2).eq("id", orderId).update();

            // 3. 手动确认死信消息，完成闭环
            channel.basicAck(deliveryTag, false);
            log.info("死信消息已成功处理并释放Redis库存资格, orderId={}", orderId);
        } catch (Exception e) {
            log.error("死信队列处理补偿机制时发生异常, orderId={}", orderId, e);
            // 补偿失败，不再放回死信队列避免死循环，可根据业务需求记录单独的告警表
            channel.basicNack(deliveryTag, false, false);
        }
    }
}

