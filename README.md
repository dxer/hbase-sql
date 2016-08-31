# hbase-sql


### select

```sql
select * from user where _rowkey_=11111
select * from user where _rowkey_ in ('1111', '2222')
```

### del

```sql
delete from user where _rowkey_=1111
delete from user where _rowkey_ in ('111','22232','3333') and _column_ in ('info.name', 'info.age')
```

### insert

```sql
insert into user (_rowkey_, info.name, info.age) values ('sdfdsfsd', 'fdsfsd', 12)
```
