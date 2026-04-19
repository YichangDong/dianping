<div align="center">
  <h1>AI+本地生活服务平台</h1>
  <p>
    <img src="https://img.shields.io/badge/Spring%20Boot-3.4.4-6DB33F" alt="Spring Boot">
    <img src="https://img.shields.io/badge/Java-17-007396" alt="Java">
    <img src="https://img.shields.io/badge/Spring%20AI-1.0.3-412991" alt="Spring AI">
    <img src="https://img.shields.io/badge/MyBatis--Plus-3.5.6-1F88E5" alt="MyBatis-Plus">
    <img src="https://img.shields.io/badge/Redisson-3.27.1-BB1E10" alt="Redisson">
    <img src="https://img.shields.io/badge/MySQL%20Connector-8.0.31-4479A1" alt="MySQL Connector">
    <img src="https://img.shields.io/badge/Redis-8.4.0-D82C20" alt="Redis">
    <img src="https://img.shields.io/badge/RabbitMQ%20Spring-3.13.7-FF6600" alt="RabbitMQ Spring">
  </p>
</div>

AI+本地生活服务平台 是一个类“大众点评”的本地生活服务平台。  
### 我在保留原本点评、优惠券、秒杀、支付这些核心业务链路的基础上，新增了：

- 使用布隆过滤器预防缓存穿透
- 使用滑动窗口策略进行限流
- 使用RabbitMQ进行异步下单
- 利用RabbitMQ的延迟消息实现定时关闭订单功能
- 使用分布式锁解决解决支付回调与超时关单状态下的并发问题