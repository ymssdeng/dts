# dts
一个简单的数据传输服务（Data Transmission Service），可用于源数据库（支持Mongo和Mysql）到目标数据库（Mysql）的全量、增量数据迁移。

DONE:
- 仅支持auto_increment主键
- 全量迁移时可指定输入ranges
- 支持mongo增量迁移(Insert)

TODO:
- 增量迁移支持设置起始和停止时间
- 支持目标数据库分表
