package com.hmdp.config;

import java.util.HashMap;
import java.util.Map;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.hmdp.utils.MqConstants;

@Configuration
public class RabbitMqConfig {

    // 业务主交换机：接收秒杀订单消息
    @Bean
    public DirectExchange seckillOrderExchange() {
        return new DirectExchange(MqConstants.SECKILL_ORDER_EXCHANGE, true, false);
    }

    // 死信交换机：处理消费失败/拒绝的消息
    @Bean
    public DirectExchange seckillOrderDlx() {
        return new DirectExchange(MqConstants.SECKILL_ORDER_DLX, true, false);
    }

    // 主队列：保存待创建的秒杀订单，并配置死信转发规则
    @Bean
    public Queue seckillOrderQueue() {
        Map<String, Object> args = new HashMap<>();
        args.put("x-dead-letter-exchange", MqConstants.SECKILL_ORDER_DLX);
        args.put("x-dead-letter-routing-key", MqConstants.SECKILL_ORDER_DLQ_ROUTING_KEY);
        return new Queue(MqConstants.SECKILL_ORDER_QUEUE, true, false, false, args);
    }

    // 死信队列：承接处理失败的订单消息，便于后续排查与补偿
    @Bean
    public Queue seckillOrderDlq() {
        return new Queue(MqConstants.SECKILL_ORDER_DLQ, true);
    }

    // 主绑定：将业务路由键绑定到主交换机与主队列
    @Bean
    public Binding seckillOrderBinding() {
        return BindingBuilder.bind(seckillOrderQueue())
                .to(seckillOrderExchange())
                .with(MqConstants.SECKILL_ORDER_ROUTING_KEY);
    }

    // 死信绑定：将死信路由键绑定到死信交换机与死信队列
    @Bean
    public Binding seckillOrderDlqBinding() {
        return BindingBuilder.bind(seckillOrderDlq())
                .to(seckillOrderDlx())
                .with(MqConstants.SECKILL_ORDER_DLQ_ROUTING_KEY);
    }

    // 使用 JSON 序列化消息，便于对象消息的发送与消费
    @Bean
    public MessageConverter messageConverter() {
        return new Jackson2JsonMessageConverter();
    }
}
