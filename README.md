# hbase-sql
最近在负责一个hbase相关的项目，项目中涉及了很多对HBase进行查询和插入的操作，随着需求不但增加，每新增一个需求，就要根据需求使用HBase API来开发来完成相应功能的开发。有没有一种办法只是通过配置的方法就可以实现对hbase的相关操作（增删改查）呢，从而减少代码的开发呢?

hbase-sql是一个基于sql语句，并将sql转化为hbase scan/put/delete操作的工具，对于开发人员，只要编写SQL语句就能实现对hbase的操作，以减少学习和开发成本。

##　hbase-sql原理
将sql语句通过sql解析器进行解析，获取sql语法节点，然后转换为hbase scan/put/delete等对象，最终调用HBase原生的API进行查询操作

hbase-sql实现了使用select，del，insert完成对hbase数据库的查找，删除，插入等操作。

### select 查询

```sql
// 获取所有列
select * from user
// 获取rowkey为11111
select * from user where _rowkey_=11111
// 查询info列族中的age列
select info.age from user where _pre_rowkey_ = 11 
select * from user where _rowkey_ in ('1111', '2222')
```

java实现如下：

```java
HBaseSqlEngine sqlEngine  = new HBaseSqlEngineImpl();
String sql = "select * from user where _rowkey_=1111";

sqlEngine.select(sql);
```

### del 删除数据

支持删除指定rowkey行的数据，支持删除指定行中某列族中的某个或多个列的数据

```sql
// 删除指定行数据
delete from user where _rowkey_=1111
// 删除指定指定行的某个列数据
delete from user where _rowkey_ in ('111','22232','3333') and _column_ in ('info.name', 'info.age')
```

java实现如下：

```java
HBaseSqlEngine sqlEngine = new HBaseSqlEngineImpl();
String s = "delete from user where _rowkey_ in ('111','22232','3333') and _column_ in ('info.name', 'info.age')";
sqlEngine.del(s);
```

### insert 插入数据

类似sql insert插入数据的方法，这里需要注意的是，需要插入rowkey，插入数据可以指定插入到某个列族中某个列。

```sql
insert into user (_rowkey_, info.name, info.age) values ('rk-1', '张三', 12)
```

java实现如下：

```java
HBaseSqlEngine sqlEngine = new HBaseSqlEngineImpl();
String s = "insert into user (_rowkey_, info.name, info.age) values ('sdfdsfsd', 'fdsfsd', 12)";
sqlEngine.insert(s);
```

ps：欢迎有兴趣的朋友一起交流学习~