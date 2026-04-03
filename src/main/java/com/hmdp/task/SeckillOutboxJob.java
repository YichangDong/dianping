package com.hmdp.task;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;

import javax.annotation.Resource;

import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.dto.VoucherOrderMessage;
import com.hmdp.entity.SeckillOutbox;
import com.hmdp.service.ISeckillOutboxService;
import com.hmdp.utils.MqConstants;

import lombok.extern.slf4j.Slf4j;

/**
 * 秒杀消息本地消息表对账任务，用于巡检长时间没有收到Confirm或者发送超时的单据
 */
@Slf4j
@Component
public class SeckillOutboxJob {

    @Resource
    private ISeckillOutboxService seckillOutboxService;

    @Resource
    private RabbitTemplate rabbitTemplate;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    private static final DefaultRedisScript<Long> SECKILL_ROLLBACK_SCRIPT;

    static {
        SECKILL_ROLLBACK_SCRIPT = new DefaultRedisScript<>();
        SECKILL_ROLLBACK_SCRIPT.setLocation(new ClassPathResource("seckill_rollback.lua"));
        SECKILL_ROLLBACK_SCRIPT.setResultType(Long.class);
    }

    /**
     * 每分钟执行一次扫描
     */
    @Scheduled(cron = "0 * * * * ?")
    public void scanAndRetryOutbox() {
        // 查询出更新时间超过1分钟，且状态仍为 0(INIT) 或者发送异常的单状态
        LocalDateTime thresholdTime = LocalDateTime.now().minusMinutes(1);

        List<SeckillOutbox> hangingMessages = seckillOutboxService.list(
                new QueryWrapper<SeckillOutbox>()
                        .in("status", 0, 2)
                        .le("update_time", thresholdTime)
                        .last("LIMIT 100") // 每次最多处理100条
        );

        if (hangingMessages == null || hangingMessages.isEmpty()) {
            return;
        }

        log.info("定时对账任务扫描到滞留消息, 数量: {}", hangingMessages.size());

        for (SeckillOutbox outbox : hangingMessages) {
            long orderId = outbox.getId();
            long userId = outbox.getUserId();
            long voucherId = outbox.getVoucherId();

            // 若是长时间停留未处理成功的，我们选择发起重发
            // 简单实现：尝试重发一次MQ，若依赖ConfirmCallback，状态会自动改变
            try {
                VoucherOrderMessage message = new VoucherOrderMessage(orderId, userId, voucherId);
                CorrelationData correlationData = new CorrelationData(String.valueOf(orderId));

                log.info("重发滞留下单消息, orderId={}", orderId);
                rabbitTemplate.convertAndSend(
                        MqConstants.SECKILL_ORDER_EXCHANGE,
                        MqConstants.SECKILL_ORDER_ROUTING_KEY,
                        message,
                        correlationData
                );

                // 将 update_time 刷新一下，避免下次立刻又扫出来，同时这也能表明重试发送了
                outbox.setUpdateTime(LocalDateTime.now());
                seckillOutboxService.updateById(outbox);

                // 更进阶的版本会在Outbox表加 retry_count，超过3次就触发回滚然后结束。我们暂时简单展示补发逻辑
            } catch (Exception e) {
                log.error("对账任务中重发消息异常, orderId={}", orderId, e);
                // 多次重发都不行（或MQ直接炸了），执行极限兜底回滚资源
                stringRedisTemplate.execute(
                        SECKILL_ROLLBACK_SCRIPT,
                        Collections.emptyList(),
                        String.valueOf(voucherId),
                        String.valueOf(userId)
                );
            }
        }
    }
}

