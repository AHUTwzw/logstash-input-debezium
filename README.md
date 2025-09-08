# Logstash Java Plugin

[![Travis Build Status](https://travis-ci.com/logstash-plugins/logstash-filter-java_filter_example.svg)](https://travis-ci.com/logstash-plugins/logstash-filter-java_filter_example)

This is a Java plugin for [Logstash](https://github.com/elastic/logstash).

It is fully free and fully open source. The license is Apache 2.0, meaning you are free to use it however you want.

The documentation for Logstash Java plugins is available [here](https://www.elastic.co/guide/en/logstash/6.7/contributing-java-plugin.html).

logstash是数据处理工具的一种，和kattle、nifi等在etl领域中有着很好的处理能力，尤其是在和ES的组合使用中，隶属elk生态也在生产实践中得到充分的验证 \
logstash-input-jdbc能够定时查询数据库数据来实现数据同步，尽管可以通过极短的调度周期实现类似实时同步的场景，但是定时查询一方面消耗数据库连接资源，在业务处理紧张时增加数据库的压力 \
那么业务上便会考虑引入canal或者flink去实现cdc的功能，但是都会带来更多的学习和运维成本 \
canal开源版本无法直接支持全量+增量的处理？otto的管理页面可以方便用户使用 \
flink支持多种数据抽取模式，似乎更好？整合dinky的情况下确实开发部署、管理也很方便 \
方案有很多，怎么选择视业务和项目的实际情况而定 \
那么logstash能否也可以实现类似的能力呢？
已知logstash是支持插件化的开发的,那么利用debezium的能力我们是可以实现一个支持cdc的input插件的，同时logstash的x-pack能力让我们可以在kibana的Logstash Pipelinesmo模块中动态配置 \
如果项目中引入了logstash和elasticsearch,不想引入过多组件增加成本或许该插件是一个好的选择？

基于logstash 9.1.0开发,使用官方logstash-input-java_input_example项目为模板 \
目前只实现mysql的cdc能力，其他数据库将来有时间再实现，其实这里应该不限定数据源，应当单独整合debezium的，数据源由外部依赖提供则更符合插件的设计 \
花费更多的时间在学习flink或者nifi上也许是更好的选择

## Todo
1:扩展更多的数据源，不限定仅mysql \
2:SMT过滤器和snapshot.mode模式需要提供更多选择 \
2:增加背压测试，看logstash的处理能力和debezium的最佳配置组合 \
3:filter插件作为清洗的核心，或许应该更多的考虑和定制化开发 \

### 本项目意在提供一个新的选择，在不引入更多组件、增加运维成本的情况下，仅用logstash也可实现cdc的能力，同时是对logstash组件的一个学习,深入理解logstash的设计与自主开发插件的能力。\
### 在实际数据同步的开发中还是需要考虑更多的情况,生产端引入更强大的组件或许是更好的选择！而消费端则由于各种业务的特殊性，自我开发一个etl或许也会比开源的etl更加好维护和实践。