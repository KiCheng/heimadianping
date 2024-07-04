package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


@Service
@Transactional
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissonClient redissonClient;

    // 代理对象
    private IVoucherOrderService proxy;

    // 创建阻塞队列
//    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));  // 默认的类加载器加载String路径下的资源
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    // 创建线程池
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    @PostConstruct  // 在类初始化后默认执行
    private void init() {
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    private class VoucherOrderHandler implements Runnable {
        String queueName = "stream.orders";
        @Override
        public void run() {
            while (true) {
                try {
                    // 1.获取消息队列中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS streams.order >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );
                    // 2、判断消息是否获取成功
                    if (list == null || list.isEmpty()) {
                        //  如果获取失败，说明没有消息，继续下一次循环
                        continue;
                    }
                    // 3、解析消息中的订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    // 4、如果获取成功，可以下单
                    handleVoucherOrder(voucherOrder);
                    // 5、ACK确认
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                    handlePendingList();
                }
            }
        }

        private void handlePendingList() {
            while (true) {
                try {
                    // 1.获取pending-list中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS stream.orders 0
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                    );
                    // 2、判断消息是否获取成功
                    if (list == null || list.isEmpty()) {
                        //  如果获取失败，说明pending-list没有消息，继续下一次循环
                        break;
                    }
                    // 3、解析消息中的订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    // 4、如果获取成功，可以下单
                    handleVoucherOrder(voucherOrder);
                    // 5、ACK确认
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
                } catch (Exception e) {
                    log.error("处理pending-list订单异常", e);
                }
            }
        }
    }

//    private class VoucherOrderHandler implements Runnable {
//        @Override
//        public void run() {
//            while (true) {
//                try {
//                    // 1.获取队列中的订单信息
//                    VoucherOrder voucherOrder = orderTasks.take();
//                    // 2.创建订单
//                    handleVoucherOrder(voucherOrder);
//                } catch (Exception e) {
//                    log.error("处理订单异常", e);
//                }
//            }
//        }
//    }

    // 创建订单
    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        boolean isLock = lock.tryLock();
        if (!isLock) {
            log.error("不允许重复下单");
            return;
        }
        try {
            // 获取事务的代理对象  -- 子线程是无法获取代理对象的
//            proxy = (IVoucherOrderService) AopContext.currentProxy();  // 子线程是无法获取代理对象的
            proxy.createVoucherOrder(voucherOrder);
        } finally {
            // 释放锁
            lock.unlock();
        }
    }

    @Override
    public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextId("order");
        // 1、执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(), String.valueOf(orderId)
        );
        int r = result.intValue();
        // 2、判断结果是否为0
        if (r != 0) {
            // 2.1 不为0，代表没有购买资格
            return Result.fail(r == 1 ? "库存不足": "同一用户不可重复下单");
        }
        // 3、获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        // 4、返回订单id
        return Result.ok(orderId);
    }

//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        Long userId = UserHolder.getUser().getId();
//        // 1、执行lua脚本
//        Long result = stringRedisTemplate.execute(SECKILL_SCRIPT, Collections.emptyList(), voucherId.toString(), userId.toString());
//        int r = result.intValue();
//        // 2、判断结果是否为0
//        if (r != 0) {
//            // 2.1 不为0，代表没有购买资格
//            return Result.fail(r == 1 ? "库存不足": "同一用户不可重复下单");
//        }
//
//        // 2.2 为0，有购买资格，将下单信息保存到阻塞队列
//        // TODO 保存到阻塞队列
//        // 创建订单
//        VoucherOrder voucherOrder = new VoucherOrder();
//        // 2.2.1 订单id
//        long orderId = redisIdWorker.nextId("order");
//        voucherOrder.setId(orderId);
//        // 2.2.2 用户id
//        voucherOrder.setUserId(UserHolder.getUser().getId());
//        // 2.2.3 秒杀券id
//        voucherOrder.setVoucherId(voucherId);
//
//        orderTasks.add(voucherOrder);  // 将订单内容加入到阻塞队列
//
//        // 3、获取代理对象
//        proxy = (IVoucherOrderService) AopContext.currentProxy();
//
//        // 4、返回订单id
//        return Result.ok(orderId);
//    }

//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        // 1、查询优惠券
//        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
//        // 2、判断秒杀是否开始
//        if (LocalDateTime.now().isBefore(voucher.getBeginTime())) {
//            return Result.fail("秒杀尚未开始！");
//        }
//        // 3、判断秒杀是否结束
//        if (LocalDateTime.now().isAfter(voucher.getEndTime())) {
//            return Result.fail("秒杀已经结束！");
//        }
//        // 4、判断库存是否充足
//        if (voucher.getStock() < 1) {
//            return Result.fail("库存不足！");
//        }
//
//        Long userId = UserHolder.getUser().getId();
//        /**
//         * 利用Redis实现集群模式下的分布式锁
//         */
//        // 创建锁对象
////        SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
//        RLock lock = redissonClient.getLock("lock:order:" + userId);
//        boolean isLock = lock.tryLock();
//        if (!isLock) {
//            return Result.fail("一个用户只 允许下一单!");
//        }
//
//        try {
//            // 获取事务的代理对象
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId);
//        } catch (IllegalStateException e) {
//            throw new RuntimeException(e);
//        } finally {
//            // 释放锁
//            lock.unlock();
//        }
//    }

    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        Long voucherId = voucherOrder.getVoucherId();
        // 5、判断一人一单
        // 5.1 用户id和优惠券id
        Long userId = voucherOrder.getUserId();

        // 5.2 判断是否存在订单
        int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
        if (count > 0) {
            // 订单已经存在
            log.error("该用户已经购买，无法重复购买！");
            return;
        }

        // 6、扣减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherId)
                .gt("stock", 0)  // 乐观锁CAS
                .update();
        if (!success) {
            log.error("扣减库存失败（库存不足）！");
            return;
        }

        // 8、订单添加到数据库
        save(voucherOrder);
    }
}
