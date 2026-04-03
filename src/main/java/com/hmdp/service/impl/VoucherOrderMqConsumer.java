package com.hmdp.service.impl;

import javax.annotation.Resource;

import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import com.hmdp.dto.VoucherOrderMessage;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.MqConstants;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class VoucherOrderMqConsumer {

    @Resource
    private IVoucherOrderService voucherOrderService;

    @Resource
    private RedissonClient redissonClient;

    @RabbitListener(queues = MqConstants.SECKILL_ORDER_QUEUE)
    public void handleVoucherOrder(VoucherOrderMessage message) {
        RLock lock = redissonClient.getLock("lock:order:" + message.getUserId());
        boolean isLock = lock.tryLock();
        if (!isLock) {
            log.warn("重复下单消息，userId={}", message.getUserId());
            return;
        }
        try {
            VoucherOrder voucherOrder = new VoucherOrder();
            voucherOrder.setId(message.getOrderId());
            voucherOrder.setUserId(message.getUserId());
            voucherOrder.setVoucherId(message.getVoucherId());
            voucherOrderService.createVoucherOrder(voucherOrder);
        } finally {
            lock.unlock();
        }
    }
}

