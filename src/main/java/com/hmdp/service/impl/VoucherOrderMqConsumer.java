package com.hmdp.service.impl;

import java.io.IOException;

import jakarta.annotation.Resource;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import com.hmdp.dto.VoucherOrderMessage;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.MqConstants;
import com.rabbitmq.client.Channel;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class VoucherOrderMqConsumer {

    @Resource
    private IVoucherOrderService voucherOrderService;

    @Resource
    private RedissonClient redissonClient;

    @RabbitListener(queues = MqConstants.SECKILL_ORDER_QUEUE)
    public void handleVoucherOrder(VoucherOrderMessage message, Channel channel, @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {
        long userId = message.getUserId();
        long orderId = message.getOrderId();

        // 使用三个独立的锁对象来模拟/实现联锁（MultiLock/RedLock思想）
        RLock lock1 = redissonClient.getLock("lock:order:node1:" + userId);
        RLock lock2 = redissonClient.getLock("lock:order:node2:" + userId);
        RLock lock3 = redissonClient.getLock("lock:order:node3:" + userId);

        // 获取联锁
        RLock lock = redissonClient.getMultiLock(lock1, lock2, lock3);

        boolean isLock = lock.tryLock();
        if (!isLock) {
            log.warn("重复下单消息(未获取到分布式锁)，userId={}", userId);
            // 业务侧认为是重复消息，直接Ack丢弃
            channel.basicAck(deliveryTag, false);
            return;
        }

        try {
            VoucherOrder voucherOrder = new VoucherOrder();
            voucherOrder.setId(orderId);
            voucherOrder.setUserId(userId);
            voucherOrder.setVoucherId(message.getVoucherId());
            voucherOrderService.createVoucherOrder(voucherOrder);

            // 成功落库后，手动确认消息
            channel.basicAck(deliveryTag, false);
            log.info("消息消费成功并ACK, orderId={}", orderId);
        } catch (Exception e) {
            // 这里判断如果是底层的 DuplicateKeyException (被 Spring 包装过的可能需要解析异常链)
            // 因为在 createVoucherOrder 内已捕获 DuplicateKeyException，这里正常不会因重复下单抛错。
            // 但如果是其他未知异常导致创建失败，拒绝并丢弃消息（配置了死信队列，会自动进入DLQ后续补偿）
            log.error("消息消费异常, orderId={}", orderId, e);
            channel.basicNack(deliveryTag, false, false);
        } finally {
            lock.unlock();
        }
    }
}
