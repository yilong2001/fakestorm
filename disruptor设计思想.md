# disruptor 设计思想与实现

- [1. 关于并发的复杂性](#1-并发复杂性)
- [2. disruptor实现](#2-Disruptor实现)


## 1并发的复杂性
### 1.1 并发的复杂性
并发，不仅仅是两个或多个任务并行发生，而是它们在竞争访问资源。被竞争的资源可能是数据库、文件、socket、或者一个内存位置。

代码的并发执行包含两个因素：互斥和改变的可见性。

(1)互斥，是怎么管理某个资源在竞争条件下的更新。

(2)改变的可见性，是当变化发生的时候，怎么使得其他线程能看到这个变化。

如果能排除竞争更新，就能排除互斥(注：这是 Disruptor设计的核心思想)。

如果你的算法能保证，任何给定的资源只会被一个线程修改，互斥就不再需要了。

读、写操作要求所有改变都对其他线程可见。然而，只有竞争写才要求对改变的互斥。

在任何并发场景下，最大的资源耗费都是竞争写。多个线程写相同的资源，要求复杂和昂贵的协作。典型的解决方案，就是使用某种类型的锁。

### 1.2 锁的成本
锁提供了互斥机制，确保了改变的可见性以有序的方式发生。锁是非常昂贵的，因为在竞争发生的时候，它们要求仲裁。仲裁通过操作系统内核的上下文切换来完成，即挂起等待锁的线程，直到锁释放。在上下文切换期间，操作系统的releasing control可以决定做一些打扫房间的任务，执行上下文将丢失以前缓存的数据和指令。在现代处理器上，这可能有一系列性能影响。能使用快速用户模式锁，但这只是在无竞争情况下才会有真正的好处。

用一个简单的实例来说明锁的消耗。实验的重点是一个函数调用，增加一个64bit的计数器，循环5亿次。用java语言，在2.4Ghz Intel Westmere EP使用单线程，执行计算需要300ms。那种语言并不重要，结果是相似的。

一旦使用锁解决互斥，即使没有资源竞争，锁的费用也是明显的。当两个或多个线程开始竞争的时候，锁的费用就会指数级增加。

### 1.3 CAS的成本
当更新的对象是一个单值，可以使用一种更有效的方法代替锁。这些代替方法有：原子操作、自旋锁等现代处理器的执行指令。更通用的名称是CAS(Compare and Swap)。CAS是一种特殊的机器码指令，允许内存中的一个字、做为原子操作、有条件地被设置。对“增加计数器实验”每个线程能在一个循环中自旋读区counter，然后尝试原子的设置counter到一个新的增加了的值。新、旧值被提供为这个指令的参数。如果，当操作被执行的时候，counter的值匹配期望的值，counter就会被更新为新值。另一方面，如果当前值不是期望的值，更新操作就会失败。这时，线程就会重新读counter并再次重试直到成功。CAS是一种比锁更有效的方法，因为它不要求内核仲裁任何上下文切换。然而，CAS也不是免费的。CPU必须锁它的指令流水线，以确保原子操作。在java，可以使用ava.util.concurrent.Atomic执行CAS操作。

如果一个程序的critical section远远比增加计数器复杂，使用多个CAS操作可能带来复杂的状态切换以协调竞争。

使用锁开发并发程序是困难的。使用CAS操作和memory barriers开发无锁算法，在很多时候会更加复杂，而且难以保证正确性。

```
Memory barrier：
程序在运行时内存实际的访问顺序和程序代码编写的访问顺序不一定一致，这就是内存乱序访问。
内存乱序访问行为出现的理由是为了提升程序运行时的性能。

内存乱序访问主要发生在两个阶段：
(1) 编译时，编译器优化导致内存乱序访问（指令重排）
(2) 运行时，多 CPU 间交互引起内存乱序访问
```

理想的情况，一个线程写单一资源，而其他线程读取结果。在多CPU环境，为了读区正确的结果，要求Memory barrier，以保证变量的变化在多CPU上运行的线程都可见。

### 1.4 Memory Barriers
出于性能的原因，现代CPU能完成指令的乱序执行和乱序装载、以及在内存和执行单元之间的数据存储。CPU只需要保证程序逻辑、生产相同结果，而不考虑执行顺序。对单线程这不是问题。然而，当线程共享状态时，内存改变的出现顺序就变得很重要，在正确执行顺序下，数据交换才是成功的。CPU使用Memory Barriers指示代码段，内存更新的顺序是重要的。Memory Barriers是线程之间物理顺序和改变可见性得以保证的方法。

```
防止编译时乱序：
#define barrier() __asm__ __volatile__("" ::: "memory")

防止变量内存访问的乱序：
volatile int x, y;
```

现代CPU运行速度比内存更快，使用复杂的缓存系统以有效利用硬件。在多个CPU缓存之间的数据修改，通过消息传输协议保证一致性。
```
有两个队列： “store buffers”， “invalidate queues”，保证多个CPU缓存之间的数据一致性。
```

CPU硬件设计为了提高指令的执行速度，增设了两个缓冲区（store buffer, invalidate queue）。这个两个缓冲区可以避免CPU在某些情况下进行不必要的等待，从而提高速度，但是这两个缓冲区的存在也同时带来了新的问题。要仔细分析这个问题需要先了解cache的工作方式。

目前CPU的cache的工作方式很像软件编程所使用的hash表，书上说“N路组相联（N-way set associative）”，其中的“组”就是hash表的模值，即hash链的个数，而常说的“N路”，就是每个链表的最大长度。链表的表项叫做 cache-line，是一段固定大小的内存块。读操作很直接，不再赘述。如果某个CPU要写数据项，必须先将该数据项从其他CPU的cache中移出， 这个操作叫做invalidation。当invalidation结束，CPU就可以安全的修改数据了。如果数据项在该CPU的cache中，但是是只读的，这个过程叫做”write miss”。一旦CPU将数据从其他CPU的cache中移除，它就可以重复的读写该数据项了。如果此时其他CPU试图访问这个数据项，将产生一 次”cache miss”，这是因为第一个CPU已经使数据项无效了。这种类型的cache-miss叫做”communication miss”，因为产生这种miss的数据项通常是做在CPU之间沟通之用，比如锁就是这样一种数据项。

为了保证在多处理器的环境下cache仍然一致，需要一种协议来防止数据不一致和丢失。目前常用的协议是MESI协议。MESI是 Modified,Exclusive, Shared, Invalid这四种状态的首字母的组合。使用该协议的cache，会在每个cache-line前加一个2位的tag，标示当前的状态。

```
modified状态：该cache-line包含修改过的数据，内存中的数据不会出现在其他CPU-cache中，此时该CPU的cache中包含的数据是最新的
exclusive状态：与modified类似，但是数据没有修改，表示内存中的数据是最新的。如果此时要从cache中剔除数据项，不需要将数据写回内存
shared状态：数据项可能在其他CPU中有重复，CPU必须在查询了其他CPU之后才可以向该cache-line写数据
invalid状态：表示该cache-line空

MESI使用消息传递在上述几种状态之间切换。如果CPU使用共享BUS，下面的消息足够：
read: 包含要读取的CACHE-LINE的物理地址
read response: 包含READ请求的数据，要么由内存满足要么由cache满足
invalidate: 包含要invalidate的cache-line的物理地址，所有其他cache必须移除相应的数据项
invalidate ack: 回复消息 
read invalidate: 包含要读取的cache-line的物理地址，同时使其他cache移除该数据。需要read response和invalidate ack消息
writeback：包含要写回的数据和地址，该状态将处于modified状态的lines写回内存，为其他数据腾出空间
```


原文如下：
```
These caches are kept coherent with other processor cache systems via message passing protocols. In addition, processors have “store buffers” to offload writes to these caches, and “invalidate queues” so that the cache coherency protocols can acknowledge invalidation messages quickly for efficiency when a write is about to happen. 
What this means for data is that the latest version of any value could, at any stage after being written, be in a register, a store buffer, one of many layers of cache, or in main memory. If threads are to share this value, it needs to be made visible in an ordered fashion and this is achieved through the coordinated exchange of cache coherency messages. The timely generation of these messages can be controlled by memory barriers. 
A read memory barrier orders load instructions on the CPU that executes it by marking a point in the invalidate queue for changes coming into its cache. This gives it a consistent view of the world for write operations ordered before the read barrier. 
A write barrier orders store instructions on the CPU that executes it by marking a point in the store buffer, thus flushing writes out via its cache. This barrier gives an ordered view to the world of what store operations happen before the write barrier.
A full memory barrier orders both loads and stores but only on the CPU that executes it.
Some CPUs have more variants in addition to these three primitives but these three are sufficient to understand the complexities of what is involved. In the Java memory model the read and write of a volatile field implements the read and write barriers respectively. This was made explicit in the Java Memory Model3 as defined with the release of Java 5.
```

### 1.5 Cache Lines
缓存在现在CPU中，提高性能的作用非常大。如果缓存命中数据或指令，CPU处理效率非常高；但是，如果缓存没有命中，则对效率的影响非常大。

出于效率，缓存被组织成cache-lines，典型大小是32-256字节，64字节最为常见。这是缓存一致性协议得以运行的基础。如果两个变量在相同的cache lines，它们被不同的线程写，就会出现写冲突的问题（就像是一个变量那样）。这被称为“false sharing”。为了高性能，确保独立、但并发写的变量不共享相同的cache lines，如果竞争被最小化。

当以可预测的方式访问内存，通过预测下一次哪个内存将被访问、而预先抓取它到缓存中，CPU能隐藏访问主内存的延迟成本。以可预测的“stride”遍历内存，CPU能探测访问方式，预测才会有效。如果是迭代访问一个数组，“stride”就是可预测的，内存数据能预先抓取到cache lines，最大化访问效率。“stride”一般小于2048个字节。然而，诸如linked lists and trees这样的数据结构有很多节点，很宽泛的分布在内存中，以至于“stride”的预测通常无效(注：这影响了Disruptor的设计)。在内存中一致性的缺乏，限制了系统预先抓取到cache lines的能力。这导致了内存访问效率下降了2个数量级。

### 1.6 队列的问题
队列一般使用linked-lists or arrays存储元素。如果内存中队列无界，它就会一直增加指导内存耗尽。当生产者超过消费者就会发生这种情况。不过，在系统中无界队列是有用的，如果能保证生产者不超过消费者。不过如果这个假设不成立，就会很危险，队列增加而无限制。为了避免灾难性结果，队列通常限制一个最大size。保持一个队列有界，要求它是基于数组的，或者size能active追踪。

队列通常在head、tail、size上面有写竞争。在使用队列的时候，因为消费者和生产者的差异，队列通常接近于full或接近于empty。生产者和消费者速率匹配的情况非常少见。队列总是空或总是满这种特性，导致了很高的竞争，以及昂贵的缓存一致性。

问题是，即使队列头和尾分别使用不同的并发对象，诸如锁或CAS变量，它们都会占用相同的cache line。

队列生产者关注点是队列头、消费者关注点是队列尾，头尾中间节点的存储使得并发设计非常复杂。在队列put和take的时候，使用一个大锁是简单的，但对吞吐量却是一个很大的瓶颈。如果这考虑队列，好像除了单生产者-单消费者，没有更好的解决办法。

在java使用队列还有一个问题，就是队列会成为GC的一个重要源头。首先，对象分配和放置在队列。其次，如果是linked list，对象必须被表示为list的节点。一旦不再使用，分配给队列的这些对象都需要收回。

### 1.7 Piplines 和 Graphs
许多问题适合使用pipline。这些pipline通常有并行路径，被组织成类似图一样的拓扑。每个阶段之间的连接常常使用队列，而且每个阶段有它自己的线程。

这个方法不便宜。每个阶段必须承担入队、出队的费用。当path必须fork时，这个费用倍增。在fork之后做re-join时，费用再次倍增。

理想的情况，即能表示依赖图，也不增加阶段之间的入队费用。


## 2 Disruptor的设计
上面提出了问题，通过分类问题的关注点、而不是把这些问题混合在一起，就会有一个清晰的设计方法。

这个方法的本质，确保数据在写的时候，只被一个线程所拥有。同样的方法，也被用于java 中的Phasers(支持Fork-Join)。

Disruptor解决了上面提出的问题，最大化内存分配的效率，以缓存友好的方式操作，以最有效地利用硬件。

Disruptor的核心机制是，以环形队列的形式，使用一个预先分配的有界数据结构。数据通过一个或多个生产者增加到ring buffer，被一个或多个消费者处理。

### 2.1 内存分配
环形队列的内存在start up的时候分配。环形队列或者是一个指针数组指向entries，或者是表示entries的数据结构的数组。Java语言的限制，意味着和环形队列关联的entries也是指针。这些entries一般不是实际数据，而是数据的容器。预先分配entries排除了GC的问题，因为entries能被重复使用。这些entries的内存同时被分配，因此很可能在主内存里面一块连续的区域上面，因此能很好的支持cache striding。类似于C语言的值类型，John Rose在java引入“值类型”，内存就能连续分配而避免指针重定向。

在低延迟系统里，GC是一个问题。GC适合于short-lived对象或immortal对象。环形队列中entries的预分配，意味着它是immortal的，所以不会对GC造成负担。

基于队列的系统，在重负载情况下，能导致处理速率的下降。结果是被分配的对象超过了预期的生命周期，因而被GC提升到年轻代之外。这有两个影响：(1)对象必须在代际之间拷贝，从而增加了延迟抖动；(2)对象必须在更老的代收集，这也会导致更多的时间耗费，增加了“stop the world”的时间。在大内存场景，这回导致几秒钟的暂停(per GB)。

### 2.2 分离关注点
剥离冗杂，关注核心，队列执行主要有三方面：
- 1、元素的存储
- 2、协调生产者获取下一个序号
- 3、协调消费者通知一个新元素的到达

在金融交易系统中，如果使用有GC的语言，太多的内存分配也是问题。所以我们认为linked-list 队列不是一个好选择。如果整个数据交换期间不同阶段的内存都能预先分配，GC的影响就会小很多。此外，如果是一个统一块的方式分配，数据的遍历就能以缓存友好的方式执行。符合这个要求的数据结构是数组，使用slot预先填充。在创建环形队列的时候，Disruptor利用了抽象工厂以预先分配entries。当一个entry被声明占用，生产者能拷贝数据到预先分配的结构中。

在大多数处理器，sequence number的剩余计算成本很高，它决定了环形的slot。使得环形大小是2的幂，能很大程度上减少这个时间成本。 Size的bit掩码减一能有效完成剩余数的计算。

如之前所述，有界队列会有head和tail的竞争。环形队列数据结构避免了这种竞争，因为，通过分离关注点，问题变成了生产者和消费者的barriers问题。barriers的问题逻辑如下：

Disruptor的通用场景，只有一个生产者。典型的，生产者是一个file reader或network listeners。这种情况，单一生产者没有sequence/entry分配的竞争。

在多个生产者的场景，生产者会彼此竞争下一个entry。这种竞争可以使用简单的sequence number的CAS操作来管理。

一旦生产者拷贝相关数据到entry，通过committing 这个sequence，就能可用给消费者。这个操作无需CAS，而是通过busy spin，直到其他生产者在他们的commit中已经到达这个sequence。这时，这个生产者能移动cursor，签名下一个可用entry写数据。生产者能够避免wrapping环形队列，通过追踪消费者sequence做为一个简单的读操作，在它们写环形队列之前。

在环形队列中，消费者等一个sequence变成可用，在读取entry之前。等待过程中，可以使用多种策略。如果CPU资源是宝贵的，可以等一个锁内部的条件变量，通过生产者触发这个信号。这明显是一个竞争点，仅仅在CPU资源比延迟、吞吐量更重要时使用。消费者也能loop checking cursor—— 表示环形队列当前可用的sequence。这能以(或者不)thread yield的方式完成，就看CPU资源和延迟哪个更重要。这种可伸缩性非常好，打破了生产者和消费者之间的竞争依赖。无锁的多生产者-多消费者队列存在，单要求在head、tail、size的多个CAS操作。Disruptor没有遭受这种CAS竞争。

### 2.3 Sequencing
Sequencing是Disruptor管理并发的核心概念。每个生产者、消费者以严格sequencing的方式，与环形队列交互。生产者在sequence得到下一个slot，在获取ring里的一个entry时。对于一个生产者，下一个可用slot的sequence只是一个简单的计数器；对多个生产者，需要CAS原子操作更新counter。一旦获取到一个sequence值，生产者就能使用在环形队列对应的entry，写入数据。当生产者完成更新entry，通过更新一个独立计数器(在环形队列表示这个 cursor ----消费者最新可用的entry)。环形队列 cursor能，生产者能以busy spin的方式读写，以memory barrier的方式，而不需要如下CSA操作：
```
long expectedSequence = claimedSequence – 1; 
while (cursor != expectedSequence) { 
// busy spin } 
        cursor = claimedSequence;
```

使用memory barrier读cursor，消费者等待给定的sequence变成可用。一旦cursor被更新，marrier update确保改变对所有消费者可见，这些消费者已经提前在等待新的cursor了。
每个消费者有自己的sequence，它们在处理完环形队列entries时更新这个值。这些消费者sequence允许生产者追踪消费者，以阻止ring溢出。消费者sequence也允许，多个消费者以一个有序的方式协调操作相同的entry。
在只有一个生产者的情况下，不考虑消费者依赖图的复杂性，不要求锁/CAS操作。整个并发，只通过memory barriers协调，基于以上讨论的序列。

### 2.4 Batching Effect
当消费者等待一个超前cursor sequence，就会出现一个队列不可能出现的有趣现象。如果消费者发现，自从它上次check、cursor已经超前了多个步骤，它能处理sequence而无并发问题。结果就是，延迟消费者能很快赶上生产者，从而平衡了系统。这种批量类型增加了吞吐量，减少和平滑了延迟。基于我们的观察，这个效果导致一个常量时间的延迟，无论负载多少，直到内存子系统饱和。这个过程符合Little’s Law。这非常不同于J曲线----在负载增加时，我们观察到的曲线。

### 2.5 Dependency Graphs
一个队列，在生产者和消费者之间，表示简单pipline的一个步骤。如果消费者形成一个类似图一样的链条，图的每个阶段之间就需要队列。这成倍增加了队列的费用。
在Disruptor，因为生产者和消费者是独立的，仅使用一个环形队列就有可能表示一个复杂的消费者依赖图。从而，很大程度上减少队列的成本，增加吞吐量、减少延时。
单一环形队列，使用复杂数据结构能用于存储entries，表示整个工作流。重点在于，如何设计这样的数据结构。以至于每个独立消费者的状态不会产生”false sharing of cache lines”。

### 2.6 Disruptor类图
以下是Disruptor framework的核心类图。这个图给了我们简化程序模型的方法。依赖图构建后，程序模型就很简单。生产者使用ProducerBarrier获取entries in sequence，写数据到entries，这时使用ProducerBarrier确认entry数据可消费。一个消费者需要提供一个BatchHandler的实现，接收新entry可用的callback。这导致程序模型类似于Actor Model(基于事件的模型)。
分离关注点允许一个更有弹性的设计。RingBuffer提供存储和数据交换，而不会产生竞争。ProducerBarrier管理获取slot的并发操作，追踪依赖的消费者以阻止ring溢出。ConsumerBarrier通知消费者，当新的entry可用时。消费者能构建成一个依赖图，以表示处理pipline的多个阶段。

### 2.7 代码示例 
```
// Callback handler which can be implemented by consumers final BatchHandler<ValueEntry> batchHandler = new BatchHandler<ValueEntry>() { 
public void onAvailable(final ValueEntry entry) throws Exception {   } 
// process a new entry as it becomes available. throws Exception results to an IO device if necessary.
public void onEndOfBatch() {   }
    // useful for flushing
public void onCompletion() {   } 
// do any necessary clean up before shutdown
}; 
RingBuffer<ValueEntry> ringBuffer = new RingBuffer<ValueEntry>(ValueEntry.ENTRY_FACTORY, SIZE, 
ClaimStrategy.Option.SINGLE_THREADED, 
WaitStrategy.Option.YIELDING); ConsumerBarrier<ValueEntry> consumerBarrier = ringBuffer.createConsumerBarrier(); 
BatchConsumer<ValueEntry> batchConsumer = new BatchConsumer<ValueEntry>(consumerBarrier, batchHandler); 
ProducerBarrier<ValueEntry> producerBarrier = ringBuffer.createProducerBarrier(batchConsumer); 
// Each consumer can run on a separate thread EXECUTOR.submit(batchConsumer); 
EXECUTOR.submit(batchConsumer);
// Producers claim entries in sequence 

ValueEntry entry = producerBarrier.nextEntry();
// copy data into the entry container

// make the entry available to consumers 

producerBarrier.commit(entry);

```
