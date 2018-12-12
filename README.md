# StructuredStreamingInSQL
Timestamp 类型格式 2018-12-07 16:34:31
### 需要的配置只有一个sql文件
#### 1.实现socket输入 console输出
配置：
```shell
CREATE TABLE SocketTable(
    word String,
    valuecount int
)WITH(
    type='socket',
    host='hadoop-sh1-core1',
    port='9998',
    delimiter=' '
);

create SINK console(
)WITH(
    type='console',
    outputmode='complete'
);

insert into console select word,count(*) from SocketTable group by word;
```
上面语句，首先创建一个table，它的前半部分是字段和类型，后半是type为socket的数据源，分隔符号是空格符(默认是逗号)，后续中会根据create的名字创建一个同名的streaming table，schema是配置的字段

然后创建sink——输出表，将console定义为一张表，type是console，outputmode为complete(默认也是)

语句，首先是一个insert into(一定要写) ，插入表就是sink表，后面则是进行处理的数据的sql，这个例子是select word,count(valuecount) from SocketTable group by word，这样，数据就能用Structured Streaming默认的流式的方式从socket到console


```shell
输入： 
a 2
a 2
输出：
Batch: 0
-------------------------------------------
+----+--------+
|WORD|count(1)|
+----+--------+
+----+--------+

-------------------------------------------
Batch: 1
-------------------------------------------
+----+--------+
|WORD|count(1)|
+----+--------+
|a   |4       |
+----+--------+
```

#### 2.实现kafka输入 console输出
```shell
CREATE TABLE kafkaTable(
    word string,
    wordcount int
)WITH(
    type='kafka',
    kafka.bootstrap.servers='dfttshowkafka001:9092',
    subscribe='test',
    group='test'
);

create SINK consoleOut(
)WITH(
    type='console',
    outputmode='complete',
    process='2s'
);

insert into consoleOut select word,count(wordcount) from kafkaTable group by word;
```
上面语句和前面一样，consoleOut配置中多了一个process='2s'，意思是，控制台2秒输出一次

#### 3.实现csv输入 console输出
```shell
CREATE TABLE csvTable(
    name string,
    age int
)WITH(
    type='csv',
    delimiter=';',
    path='F:\E\wordspace\sqlstream\filepath'
);

create SINK console(
)WITH(
    type='console',
    outputmode='complete',
);

insert into console select name,sum(age) from csvTable group by name;


输入的csv文件里的数据是：

zhang;23
wang;24
li;25
zhang;56

输出是：

root
 |-- NAME: string (nullable = true)
 |-- AGE: integer (nullable = true)

-------------------------------------------
Batch: 0
-------------------------------------------
+-----+--------+
|NAME |sum(AGE)|
+-----+--------+
|zhang|79      |
|wang |24      |
|li   |25      |
+-----+--------+
```
#### 4.实现socket输入 console输出,添加processtime的窗口函数
```shell
CREATE TABLE SocketTable(
    word String
)WITH(
    type='socket',
    host='hadoop-sh1-core1',
    processwindow='10 seconds,5 seconds',
    watermark='10 seconds',
    port='9998'
);

create SINK console(
)WITH(
    type='console',
    outputmode='complete'
);

insert into console select processwindow,word,count(*) from SocketTable group by processwindow,word;
```
上面socket中多了两个参数，processwindow和watermark，processwindow其实就和sparkstreaming的流式处理差不多，前面是window，后一个是slide，写一个或者两个一致都是翻转窗口。

watermark是一个延迟，就是允许你的数据迟到多久，这个，貌似在processtime里没啥意义。

sql语句中，processwindow其实包含两个值，window的起始和结束，我们看一下结果
```shell
-------------------------------------------
Batch: 0
-------------------------------------------
+-------------+----+--------+
|PROCESSWINDOW|WORD|count(1)|
+-------------+----+--------+
+-------------+----+--------+

-------------------------------------------
Batch: 1
-------------------------------------------
+------------------------------------------+----+--------+
|PROCESSWINDOW                             |WORD|count(1)|
+------------------------------------------+----+--------+
|[2018-12-11 19:17:00, 2018-12-11 19:17:10]|c   |1       |
|[2018-12-11 19:17:00, 2018-12-11 19:17:10]|a   |3       |
+------------------------------------------+----+--------+

-------------------------------------------
Batch: 2
-------------------------------------------
+------------------------------------------+----+--------+
|PROCESSWINDOW                             |WORD|count(1)|
+------------------------------------------+----+--------+
|[2018-12-11 19:17:00, 2018-12-11 19:17:10]|c   |2       |
|[2018-12-11 19:17:00, 2018-12-11 19:17:10]|a   |4       |
+------------------------------------------+----+--------+

```
sql中select部分也可以不加processwindow则去掉PROCESSWINDOW这个参数，但是group部分要加上去，这样才能做到根据窗口分组数据

#### 4.实现socket输入 console输出,添加eventtime的窗口函数
eventtime和processtime的区别主要是，eventtime是根据事件事件来处理数据的，process则是来一条处理一条
```shell
CREATE TABLE SocketTable(
    timestamp Timestamp,
    word String
)WITH(
    type='socket',
    host='hadoop-sh1-core1',
    eventfield='timestamp',
    eventwindow='10 seconds,5 seconds',
    watermark='10 seconds',
    port='9998'
);

create SINK console(
)WITH(
    type='console',
    outputmode='complete'
);

insert into console select eventwindow,word,count(*) from SocketTable group by eventwindow,word;
```
eventtime——>根据事件事件生成，你的数据中肯定要有一个字段是代表时间的，上面的例子中代表时间字段的就是timestamp字段，类型是Timestamp

再下半部分的配置中有个eventfield的配置，就是指定前面的field中哪一个用来作为事件时间的那个时间

eventwindow和processtime的意思差不多名字不一样而已

watermark就是允许事件延迟的时间了，因为根据事件时间处理，肯定会存在先来后到，watermark设置为10 seconds，就是允许你的record的时间延迟10秒，后面，超过10秒的数据，再迟来的话，就会被丢弃。

```shell
运行过程中打印的schema
root
 |-- TIMESTAMP: timestamp (nullable = true)
 |-- WORD: string (nullable = true)
 |-- eventwindow: struct (nullable = true)
 |    |-- start: timestamp (nullable = true)
 |    |-- end: timestamp (nullable = true)
 
输入数据
2018-12-07 16:36:12,a
2018-12-07 16:36:22,a
2018-12-07 16:36:32,b
2018-12-07 16:36:42,a
2018-12-07 16:36:52,a

输出结果
Batch: 0
-------------------------------------------
+-----------+----+--------+
|EVENTWINDOW|WORD|count(1)|
+-----------+----+--------+
+-----------+----+--------+

-------------------------------------------
Batch: 1
-------------------------------------------
+------------------------------------------+----+--------+
|EVENTWINDOW                               |WORD|count(1)|
+------------------------------------------+----+--------+
|[2018-12-07 16:36:05, 2018-12-07 16:36:15]|a   |1       |
|[2018-12-07 16:36:10, 2018-12-07 16:36:20]|a   |1       |
+------------------------------------------+----+--------+

-------------------------------------------
Batch: 2
-------------------------------------------
+------------------------------------------+----+--------+
|EVENTWINDOW                               |WORD|count(1)|
+------------------------------------------+----+--------+
|[2018-12-07 16:36:30, 2018-12-07 16:36:40]|b   |1       |
|[2018-12-07 16:36:15, 2018-12-07 16:36:25]|a   |1       |
|[2018-12-07 16:36:45, 2018-12-07 16:36:55]|a   |1       |
|[2018-12-07 16:36:40, 2018-12-07 16:36:50]|a   |1       |
|[2018-12-07 16:36:20, 2018-12-07 16:36:30]|a   |1       |
|[2018-12-07 16:36:50, 2018-12-07 16:37:00]|a   |1       |
|[2018-12-07 16:36:25, 2018-12-07 16:36:35]|b   |1       |
|[2018-12-07 16:36:05, 2018-12-07 16:36:15]|a   |1       |
|[2018-12-07 16:36:10, 2018-12-07 16:36:20]|a   |1       |
|[2018-12-07 16:36:35, 2018-12-07 16:36:45]|a   |1       |
+------------------------------------------+----+--------+

```

#### 5.改变sql语句而不用重启项目实现更新(待实现)

#### 6.配置中加入spark的配置参数实现调优(待实现)

#### 7.自定义UDF函数(待实现)

