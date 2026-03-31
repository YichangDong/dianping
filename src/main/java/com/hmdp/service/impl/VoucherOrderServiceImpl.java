package com.hmdp.service.impl;

import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.service.IVoucherService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;

import cn.hutool.core.bean.BeanUtil;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Resource;

import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private IVoucherService voucherService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private RedissonClient redissonClient;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    private class VoucherOrderHandler implements Runnable {
        String queueName = "stream.orders";
        @Override
        public void run() {
            while (true) {
                try {
                    // 1.获取消息队列中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS stream.orders >
                    List<MapRecord<String,Object,Object>> list = stringRedisTemplate.opsForStream().read(
                        Consumer.from("g1", "c1"),
                        StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                        StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );
                    // 2.判断消息是否获取成功
                    if(list == null || list.isEmpty()) {
                        // 3.如果获取失败，说明没有消息，继续下一次循环
                        continue;
                    }
                    // 4.如果获取成功，创建订单
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    handleVoucherOrder(voucherOrder);
                    // 5.确认消息已经被消费 XACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                    handlePendingList();
                }
            }
        }
    }

    private void handlePendingList() {
        String queueName = "stream.orders";
        while (true) {
            try {
                // 1.获取pending-list中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 STREAMS stream.orders 0
                List<MapRecord<String,Object,Object>> list = stringRedisTemplate.opsForStream().read(
                    Consumer.from("g1", "c1"),
                    StreamReadOptions.empty().count(1),
                    StreamOffset.create(queueName, ReadOffset.from("0"))
                );
                // 2.判断消息是否获取成功
                if(list == null || list.isEmpty()) {
                    // 3.如果获取失败，说明没有异常消息，结束循环
                    break;
                }
                // 4.如果获取成功，创建订单
                MapRecord<String, Object, Object> record = list.get(0);
                Map<Object, Object> values = record.getValue();
                VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                handleVoucherOrder(voucherOrder);
                // 5.确认消息已经被消费 XACK stream.orders g1 id
                stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
            } catch (Exception e) {
                log.error("处理pending-list订单异常", e);
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }
        }
    }

    // private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);
    // private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newFixedThreadPool(10);
    // @PostConstruct
    // public void init() {
    //     SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    // }
    // private class VoucherOrderHandler implements Runnable {
    //     @Override
    //     public void run() {
    //         while (true) {
    //             try {
    //                 // 1.获取阻塞队列中的订单信息
    //                 VoucherOrder voucherOrder = orderTasks.take();
    //                 // 2.创建订单
    //                 handleVoucherOrder(voucherOrder);
    //             } catch (Exception e) {
    //                 log.error("处理订单异常", e);
    //             }
    //         }
    //     }
    // }

    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        // 1.获取用户id
        Long userId = voucherOrder.getUserId();
        // 2.创建锁对象
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        // 3.获取锁
        boolean isLock = lock.tryLock();
        // 4.判断是否获取锁成功
        if (!isLock) {
            // 获取锁失败，直接返回失败
            log.error("不允许重复下单");
            return;
        }
        try {
            proxy.createVoucherOrder(voucherOrder);
        } finally {
            lock.unlock();
        }
    }

    private IVoucherOrderService proxy;
    @Override
    public Result seckillVoucher(Long voucherId){
        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextId("order");

        // 1.执行lua脚本
        Long result = stringRedisTemplate.execute(SECKILL_SCRIPT, 
            Collections.emptyList(), 
            voucherId.toString(),
            userId.toString(),
            String.valueOf(orderId)
        );

        // 2.判断结果
        int r = result.intValue();
        if (r != 0) {
            // 2.1.不为0，代表没有购买资格
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }

        proxy = (IVoucherOrderService) AopContext.currentProxy();

        return Result.ok(orderId);
    }
    // @Override
    // public Result seckillVoucher(Long voucherId){
    //     // 1.执行lua脚本
    //     Long result = stringRedisTemplate.execute(SECKILL_SCRIPT, 
    //         Collections.emptyList(), 
    //         voucherId.toString(),
    //         UserHolder.getUser().getId().toString());

    //     // 2.判断结果
    //     int r = result.intValue();
    //     if (r != 0) {
    //         // 2.1.不为0，代表没有购买资格
    //         return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
    //     }
    //     // 2.2.为0，有购买资格，把下单信息存入阻塞队列
    //     VoucherOrder voucherOrder = new VoucherOrder();
    //     voucherOrder.setUserId(UserHolder.getUser().getId());
    //     voucherOrder.setVoucherId(voucherId);
    //     orderTasks.add(voucherOrder);

    //     proxy.createVoucherOrder(voucherOrder);        

    //     return Result.ok(voucherOrder);
    // }
    // @Override
    // public Result seckillVoucher(Long voucherId) throws InterruptedException {
    //     // 1.查询优惠券信息
    //     Voucher voucher = voucherService.getById(voucherId);
    //     // 2.判断秒杀是否开始
    //     if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
    //         // 秒杀未开始
    //         return Result.fail("秒杀未开始");
    //     }
    //     if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
    //         // 秒杀已结束
    //         return Result.fail("秒杀已结束");
    //     }
    //     // 3.判断库存是否充足
    //     if (voucher.getStock() < 1) {
    //         // 库存不足
    //         return Result.fail("库存不足");
    //     }
        
    //     Long userId = UserHolder.getUser().getId();
    //     // 创建锁对象
    //     RLock lock = redissonClient.getLock("order:" + userId);
    //     // 获取锁
    //     boolean isLock = lock.tryLock(1, 10, TimeUnit.SECONDS);
    //     // 判断是否获取锁成功 
    //     if (!isLock) {
    //         // 获取锁失败，返回错误信息
    //         return Result.fail("请勿重复下单");
    //     }
    //     try {
    //         // 获取代理对象（事务）
    //         IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
    //         // 执行创建订单的方法
    //         return proxy.createVoucherOrder(voucherId);
    //     } finally {
    //         // 释放锁
    //         lock.unlock();
    //     }
    // }

    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        // 4.根据优惠券id和用户id查询订单
        long userId = voucherOrder.getUserId();

        // 5.判断用户是否已经购买过了
        Integer count = query().eq("voucher_id", voucherOrder.getVoucherId()).eq("user_id", userId).count();
        if (count > 0) {
            // 用户已经购买过了
            log.error("用户已经购买过一次了");
            return;
        }
        // 6.扣减库存
        boolean success = voucherService.update().setSql("stock = stock - 1").eq("id", voucherOrder.getVoucherId()).gt("stock", 0).update();
        if (!success) {
            // 扣减库存失败
            log.error("库存不足");
            return;
        }
        // 7.创建订单
        save(voucherOrder);
    }
}
