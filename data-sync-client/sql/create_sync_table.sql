CREATE TABLE `sync_base_config`
(
    `id`                    int(10) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
    `db`                    varchar(20)  NOT NULL DEFAULT '' COMMENT '源数据库名',
    `table`                 varchar(45)  NOT NULL DEFAULT '' COMMENT '源数据表名',
    `update_compare_fields` varchar(500) NOT NULL DEFAULT '' COMMENT '更新时比较的字段，多个用","分割',
    `update_by_query`       smallint(2) NOT NULL DEFAULT '0' COMMENT '查询更新，0 否，1 是',
    `del_key_name`          varchar(45)  NOT NULL DEFAULT '' COMMENT '逻辑删除键名',
    `has_del_val`           varchar(10)  NOT NULL DEFAULT '' COMMENT '逻辑删除"已删除"对应的值',
    `del_whole_data`        smallint(2) NOT NULL DEFAULT '0' COMMENT '删除操作会删除关联整条数据，0 否（更新），1 是，主表通常要设为1',
    `is_main_table`         smallint(2) NOT NULL DEFAULT '0' COMMENT '是否主表，0 否，1 是',
    `create_time`           datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `update_time`           datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    `is_del`                smallint(2) NOT NULL DEFAULT '0' COMMENT '是否删除，0 否，1 是',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据同步设置基础表';

CREATE TABLE `sync_es_writer_config`
(
    `id`                      int(11) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
    `base_conf_id`            int(11) unsigned NOT NULL DEFAULT '0' COMMENT '基础配置id',
    `update_script`           varchar(500) NOT NULL DEFAULT '' COMMENT 'es更新script，参数用%field_name%封装，无需对字符类型设置''''',
    `es_index`                varchar(45)  NOT NULL DEFAULT '' COMMENT 'es index名称',
    `es_type`                 varchar(20)  NOT NULL DEFAULT '' COMMENT 'es type名称',
    `es_routing`              varchar(45)  NOT NULL DEFAULT '' COMMENT 'es routing key',
    `es_id_name`              varchar(45)  NOT NULL DEFAULT '' COMMENT 'es主键id名称，对应sql查询字段',
    `id_origin_name`          varchar(45)  NOT NULL DEFAULT '' COMMENT '主键在表中的名称，用于删除操作获取主键值',
    `es_id_prefix`            varchar(45)  NOT NULL DEFAULT '' COMMENT 'es主键id前缀，主表时用',
    `es_foreign_key_name`     varchar(45)  NOT NULL DEFAULT '' COMMENT '从表更新时，关联的外键名称',
    `foreign_key_origin_name` varchar(45)  NOT NULL DEFAULT '' COMMENT '关联键在表中的字段名，用于删除操作获取关联值',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=8 DEFAULT CHARSET=utf8mb4 COMMENT='同步es写入配置';

CREATE TABLE `sync_sql_reader_config`
(
    `id`               int(11) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
    `base_conf_id`     int(11) unsigned NOT NULL DEFAULT '0' COMMENT '基础配置id',
    `query_whole_sql`  text NOT NULL COMMENT '查询整条数据sql，参数用#{field_name}封装',
    `query_update_sql` text NOT NULL COMMENT '查询更新数据sql，主表无需设置，注意查询结果为单一数据，参数用#{field_name}封装',
    `query_delete_sql` text NOT NULL COMMENT '删除操作时，查询整条数据的sql，参数用#{field_name}封装',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=8 DEFAULT CHARSET=utf8mb4 COMMENT='同步sql读取配置';

