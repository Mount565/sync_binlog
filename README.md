# sync_binlog
基于mysqlfailover实现的完美failover方案

配合mysqlfailover的--exec-before 注入脚本，同步masterbinlog到slave， 从而使slave恢复过程中binlog无缺失
