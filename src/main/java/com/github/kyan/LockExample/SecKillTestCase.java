package com.github.kyan.LockExample;

import com.github.kyan.RedisLock;

import java.time.LocalTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 秒杀测试案例
 *
 * 该测试案例假设每个参与秒杀的线程都会坚持到最后直到商品被抢购完
 */
public class SecKillTestCase {

    public static void main(String[] args) {
        //定义秒杀的商品名和总数
        SecKillService secKillService = new SecKillService("foobar", 10);
        ExecutorService pool = Executors.newCachedThreadPool();
        //模拟1000个线程参与秒杀
        for (int i = 0; i < 1000; i++) {
            pool.execute(() -> {
                new BuyHandlerThread(secKillService).run();
            });
        }
        pool.shutdown();
        while (true) {
            if (pool.isTerminated()) {
                System.out.println("All threads terminated");
                break;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}

class SecKillService {

    RedisLock redisLock;
    int left;

    public SecKillService(String productName, int total) {
        redisLock = new RedisLock(productName);
        left = total;
    }

    public boolean handlerOrder() throws InterruptedException {
        if (left <= 0) {
            System.out.println("Product already sold out!");
            return true;
        }
        if (redisLock.tryLock(30000)) {
            if (left > 0) {
                --left;
                System.out.println(String.format("Thread: %s get product success, %d left; Time at: %s",
                        Thread.currentThread().getId(), left, LocalTime.now()));
            } else {
                System.out.println(String.format("Tread: %s get product failed, product sold out!", Thread.currentThread().getId()));
            }
            redisLock.unlock();
            return true;
        }
        return false;
    }
}

class BuyHandlerThread implements Runnable {


    SecKillService service;

    public BuyHandlerThread(SecKillService secKillService) {
        service = secKillService;
    }

    @Override
    public void run() {
        while (true) {
            try {
                Thread.sleep((long) (Math.random() * 5000));
                boolean ret = service.handlerOrder();
                if (ret) break;
            } catch (InterruptedException e) {
            }
        }
    }
}


