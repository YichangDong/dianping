package com.hmdp.service.impl;

import java.util.Collections;

import javax.annotation.Resource;

import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.dto.VoucherOrderMessage;
import com.hmdp.entity.SeckillOutbox;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillOutboxService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.service.IVoucherService;
import com.hmdp.utils.MqConstants;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;


/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private IVoucherService voucherService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private RabbitTemplate rabbitTemplate;

    @Resource
    private ISeckillOutboxService seckillOutboxService;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    private static final DefaultRedisScript<Long> SECKILL_ROLLBACK_SCRIPT;

    static {
        // 秒杀资格校验与预扣库存脚本
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);

        // 发送MQ失败时，回滚Redis预扣库存与下单标记
        SECKILL_ROLLBACK_SCRIPT = new DefaultRedisScript<>();
        SECKILL_ROLLBACK_SCRIPT.setLocation(new ClassPathResource("seckill_rollback.lua"));
        SECKILL_ROLLBACK_SCRIPT.setResultType(Long.class);
    }

    @Override
    public Result seckillVoucher(Long voucherId) {
        // 获取当前用户并生成订单ID（先响应订单号，后异步落库）
        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextId("order");

        // 执行Lua脚本：原子完成库存校验 + 一人一单校验 + 预扣库存
        Long executeResult = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString());
        if (executeResult == null) {
            return Result.fail("抢购失败，请重试");
        }

        // 脚本返回：0成功，1库存不足，2重复下单
        int r = executeResult.intValue();
        if (r != 0) {
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }

        try {
            // 资格校验通过后，先落库本地消息表(Outbox)
            SeckillOutbox outbox = new SeckillOutbox();
            outbox.setId(orderId);
            outbox.setUserId(userId);
            outbox.setVoucherId(voucherId);
            outbox.setStatus(0); // 0-INIT
            seckillOutboxService.save(outbox);

            // 发送异步下单消息，带有相关数据以便回调确认
            VoucherOrderMessage message = new VoucherOrderMessage(orderId, userId, voucherId);
            CorrelationData correlationData = new CorrelationData(String.valueOf(orderId));
            rabbitTemplate.convertAndSend(
                    MqConstants.SECKILL_ORDER_EXCHANGE,
                    MqConstants.SECKILL_ORDER_ROUTING_KEY,
                    message,
                    correlationData);
            return Result.ok(orderId);
        } catch (Exception e) {
            // 本地落库或消息发送异常：回滚Redis预扣，避免长期不一致
            log.error("尝试落库并发送下单消息失败, orderId={}", orderId, e);
            stringRedisTemplate.execute(
                    SECKILL_ROLLBACK_SCRIPT,
                    Collections.emptyList(),
                    voucherId.toString(),
                    userId.toString());
            // 如果Outbox落库失败，事务会回滚/脏数据不产生（这里若有完整事务更好，但因无Spring事务注解，暂依靠兜底回滚）
            return Result.fail("抢购人数过多，请稍后重试");
        }
    }

    @Override
    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        long userId = voucherOrder.getUserId();

        // 再次校验一人一单，防止消息重复消费导致重复下单
        Integer count = query()
                .eq("voucher_id", voucherOrder.getVoucherId())
                .eq("user_id", userId)
                .count();
        if (count > 0) {
            log.warn("用户重复下单");
            return;
        }

        // 扣减数据库库存，利用 stock > 0 作为并发安全条件
        boolean success = voucherService.update()
                .setSql("stock = stock - 1")
                .eq("id", voucherOrder.getVoucherId())
                .gt("stock", 0)
                .update();
        if (!success) {
            log.warn("库存不足");
            return;
        }

        try {
            // 创建订单，配合唯一索引兜底幂等
            save(voucherOrder);
        } catch (DuplicateKeyException e) {
            log.warn("订单幂等命中");
        }
    }
}
