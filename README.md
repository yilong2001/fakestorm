# fakestorm
fake storm for study and learn  (totally rewrite with java)

# fakestorm
为深入研究storm，使用 java 完全重写的一个 fake storm

###License
* Apache License 2.0 

###说明
*    为深入理解storm，易于学习和分享，完全用java重写了一个fake storm；
*    fake storm 在功能上与 storm 基本相同，只是出于研究目的，做了相应的简化；

*    原生 storm 应用程序，不需要修改，可以直接在fake storm上运行；
*    为了说明，同时附带有 word count 例子；Submit topo的接口，需要调用 fake storm 的 submit 接口，才能提交到 fake storm 服务中运行；

###差异和特性
*    使用 redis 代替zookeeper，主要考虑的是：
*    1、redis不仅可以作为分布式锁，用于 nimbus 选择 leader；
*    2、redis还可以用作消息队列，这样原生storm的localStore功能可以减轻，有很多内容可以直接存储在redis里面(例如：配置、jar包等等)；
*    3、redis使用和部署也比较简单一些；
*    4、当然，从高可用的角度看redis比zookeeper会差一些；不过在日志处理等比较简单的应用场景，redis基本能满足需求；

*    目前版本中，nimbus 没实现 leader 选举功能；
*    目前版本中，nimbus 的资源调度只是非常简单的一个例子；

*    worker之间的通信，使用了 Nifty（https://github.com/facebook/nifty)，是 thrift 和 netty 简单完美的一个集成；
*    基于disruptor的良好性能，继续使用 disruptor 作为 worker 内部的消息队列；

*    如果用于研究学习，可以在一个节点启动nimbus、supervisor进程，supervisor 会根据配置 ports，启动相应的worker；

###讨论&交流
*    mail: yilong2001@126.com
*    WX: yilong2001
