package com.hmdp.config;

import com.hmdp.service.ISeckillOutboxService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

@Slf4j
@Configuration
public class RabbitTemplateConfig implements ApplicationContextAware {

    private ApplicationContext applicationContext;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    @PostConstruct
    public void init() {
        RabbitTemplate rabbitTemplate = applicationContext.getBean(RabbitTemplate.class);
        ISeckillOutboxService outboxService = applicationContext.getBean(ISeckillOutboxService.class);

        // 设置ConfirmCallback
        rabbitTemplate.setConfirmCallback((correlationData, ack, cause) -> {
            if (correlationData == null || correlationData.getId() == null) {
                return;
            }
            Long orderId = Long.valueOf(correlationData.getId());
            if (ack) {
                // 消息发送到交换机成功，更新状态为SUCCESS
                log.info("消息发送成功, orderId={}", orderId);
                outboxService.update().set("status", 1).eq("id", orderId).update();
            } else {
                // 消息发送到交换机失败，更新状态为FAIL
                log.error("消息发送失败, orderId={}, 原因={}", orderId, cause);
                outboxService.update().set("status", 2).eq("id", orderId).update();
            }
        });

        // 设置ReturnCallback (Spring AMQP 2.x)
        rabbitTemplate.setReturnCallback((message, replyCode, replyText, exchange, routingKey) -> {
            // 消息路由到队列失败（交换机收到了但没进队列）
            log.error("消息路由失败, 状态码: {}, 描述: {}, 交换机: {}, 路由键: {}",
                    replyCode, replyText, exchange, routingKey);
            // 由于ReturnCallback没有自带correlationData，通常这被认为是由于配置导致的严重故障
        });
    }
}
