package com.hmdp.utils;

public interface Ilock {
    /*
    * 获取锁
    * @param timeoutSec 锁的过期时间，单位秒
    * @return true成功 false失败
     */
    boolean tryLock(long timeoutSec);
    /*
    * 释放锁
    * @param key 锁的key
     */
    void unLock();
}
