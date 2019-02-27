# sync_binlog
基于mysqlfailover实现的完美failover方案

配合mysqlfailover的--exec-before 注入脚本，同步masterbinlog到slave， 从而使slave恢复过程中binlog无缺失

MHA 0.58 针对GTID复制不会再从故障master上做binlog补偿 [source](https://github.com/yoshinorim/mha4mysql-manager/blob/abe11f9abb74bc5886012ff5cc9e209fb9b093c1/lib/MHA/MasterFailover.pm#L2131) 但针对GTID auto_position复制还是有方案来避免主库上最后一个事务未被复制到从库的。  

#方案一：

从故障master上获取最后一个binlog ,然后复制到备选master上执行！因为启用了GTID，已经执行过的事务会被自动跳过，只有未执行的事务才会得到执行。
```
mysqlbinlog binlog.000040 | /usr/local/mysql/bin/mysql --socket=/dbfiles/mysql_home/data/mysql.sock -uroot
```
缺点是恢复时间增长，因为解析和执行binlog需要时间。
#方案二：
从备选slave上找到所复制到的最新GTID（`Retrieved_Gtid_Set`）,然后从故障master的binlog中解析出这个GTID set以后的事务，并将这些事务在备选master上执行。
