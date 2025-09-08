package org.logstashplugins;

import co.elastic.logstash.api.*;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

// class name must match plugin name
@LogstashPlugin(name="java_input_debezium")
public class InputDebezium implements Input {

    public static final PluginConfigSpec<String> TOPIC_PREFIX =
            PluginConfigSpec.stringSetting("topic_prefix", "debezium_mysql-");

    public static final PluginConfigSpec<String> DATABASE_HOSTNAME =
            PluginConfigSpec.stringSetting("database_hostname", "");

    public static final PluginConfigSpec<String> DATABASE_USER =
            PluginConfigSpec.stringSetting("database_user", "");

    public static final PluginConfigSpec<String> DATABASE_PASSWORD =
            PluginConfigSpec.stringSetting("database_password", "");

    public static final PluginConfigSpec<String> DATABASE_PORT =
            PluginConfigSpec.stringSetting("database_port", "3306");

    public static final PluginConfigSpec<String> DATABASE_SERVER_ID =
            PluginConfigSpec.stringSetting("database_server_id", "184055");

    public static final PluginConfigSpec<String> DATABASE_SERVER_NAME =
            PluginConfigSpec.stringSetting("database_server_name", "mysql-server");

    public static final PluginConfigSpec<String> DATABASE_INCLUDE_LIST =
            PluginConfigSpec.stringSetting("database_include_list", "*");

    public static final PluginConfigSpec<String> TABLE_INCLUDE_LIST =
            PluginConfigSpec.stringSetting("table_include_list", "*");

    public static final PluginConfigSpec<String> SNAPSHOT_MODE =
            PluginConfigSpec.stringSetting("snapshot_mode", "initial");

    public static final PluginConfigSpec<String> OFFSET_FLUSH_INTERVAL_MS =
            PluginConfigSpec.stringSetting("offset_flush_interval_ms", "5000");

    public static final PluginConfigSpec<String> OFFSET_STORAGE_FILE_FILENAME =
            PluginConfigSpec.stringSetting("offset_storage_file_fileName", "mysql-offset.dat");

    public static final PluginConfigSpec<String> INCLUDE_SCHEMA_CHANGES =
            PluginConfigSpec.stringSetting("include_schema_changes", "false");

    public static final PluginConfigSpec<String> ONLY_DDL =
            PluginConfigSpec.stringSetting("ONLY_DDL", "false");

    private final String id;
    private final String topicPrefix;
    private final String databaseHostName;
    private final String databaseUser;
    private final String databasePassword;
    private final String databasePort;
    private final String databaseServerId;
    private final String databaseServerName;
    private final String databaseIncludeList;
    private final String tableIncludeList;
    private final String snapshotMode;
    private final String offsetFlushIntervalMs;
    private final String offsetStorageFileFileName;
    private final String includeSchemaChanges;
    private final String schemaHistoryIntervalStoreOnlyCapturedTablesDdl;
    private String maxBatchSize;
    private DebeziumEngine<ChangeEvent<String, String>> engine;
    private ExecutorService executor;

    // all plugins must provide a constructor that accepts id, Configuration, and Context
    public InputDebezium(String id, Configuration config, Context context) {
        // constructors should validate configuration options
        this.id = id;
        topicPrefix = config.get(TOPIC_PREFIX);
        databaseHostName = config.get(DATABASE_HOSTNAME);
        databaseUser = config.get(DATABASE_USER);
        databasePassword = config.get(DATABASE_PASSWORD);
        databasePort = config.get(DATABASE_PORT);
        databaseServerId = config.get(DATABASE_SERVER_ID);
        databaseServerName = config.get(DATABASE_SERVER_NAME);
        databaseIncludeList = config.get(DATABASE_INCLUDE_LIST);
        tableIncludeList = config.get(TABLE_INCLUDE_LIST);
        snapshotMode = config.get(SNAPSHOT_MODE);
        offsetFlushIntervalMs = config.get(OFFSET_FLUSH_INTERVAL_MS);
        offsetStorageFileFileName = config.get(OFFSET_STORAGE_FILE_FILENAME);
        includeSchemaChanges = config.get(INCLUDE_SCHEMA_CHANGES);
        schemaHistoryIntervalStoreOnlyCapturedTablesDdl = config.get(ONLY_DDL);
    }

    @Override
    public void start(Consumer<Map<String, Object>> consumer) {
        // 1. 配置Debezium连接器属性
        Properties props = configureDebeziumProperties();

        // 2. 创建Debezium引擎
        engine = DebeziumEngine
                .create(Json.class)
                .using(props)
                .notifying(record -> {
                    Map<String, Object> event = new HashMap<>();
                    event.put("key", record.key());
                    event.put("value", record.value());
                    consumer.accept(event);
                })
                .build();

        // 3. 启动引擎
        executor = Executors.newSingleThreadExecutor();
        executor.execute(engine);
    }

    private Properties configureDebeziumProperties() {
        Properties props = new Properties();
        // 连接器基本配置
        // name可以是任务名称，任务启动后会生成相同名称的线程名
        props.setProperty("name", "mysql-connector");
        //必填项，指定topic名称前缀，虽然这里没用到kafka, 但是必须配置，否则会报错
        props.setProperty("topic.prefix", topicPrefix);

        // mysql连接器全限定名，其他数据库类型时需要更换
        props.setProperty("connector.class", "io.debezium.connector.mysql.MySqlConnector");

        // 要监听的数据库连接信息
        props.setProperty("database.hostname", databaseHostName);
        props.setProperty("database.user", databaseUser);
        props.setProperty("database.password", databasePassword);
        props.setProperty("database.port", databasePort);
        //伪装成mysql从服务器的唯一Id，serverId冲突会导致其他进程被挤掉
        props.setProperty("database.server.id", databaseServerId);
        props.setProperty("database.server.name", databaseServerName);

        // 监听的数据库
        props.setProperty("database.include.list", databaseIncludeList);
        // 监听的表
        props.setProperty("table.include.list", tableIncludeList);

        // 快照模式
        // initial（历史+增量）
        // initial_only（仅读历史）
        // no_data（同schema_only） 仅增量，读取表结构的历史和捕获增量，以及表数据的增量
        // schema_only_recovery(同recovery) 从指定offset恢复读取，暂未实现
        props.setProperty("snapshot.mode", snapshotMode);

        // 偏移量刷新间隔
        props.setProperty("offset.flush.interval.ms", offsetFlushIntervalMs);
        // 偏移量存储 文件
        props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
        // 会在项目目录下生成文件
        props.setProperty("offset.storage.file.filename", offsetStorageFileFileName);

        // 是否包含数据库表结构层面的变更 默认值true
        props.setProperty("include.schema.changes", includeSchemaChanges);
        // 是否仅监听指定表的ddl变更 默认值false, false会监听所有schema的表结构变更
        props.setProperty("schema.history.internal.store.only.captured.tables.ddl", schemaHistoryIntervalStoreOnlyCapturedTablesDdl);

        // 表结构历史存储（可选）
        props.setProperty("schema.history.internal", "io.debezium.storage.file.history.FileSchemaHistory");
        props.setProperty("schema.history.internal.file.filename", "schema-history.dat");

        // Debezium 3.2新特性: 启用新的记录格式
        props.setProperty("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        props.setProperty("value.converter.schemas.enable", "false");

        // 背压控制配置
        props.setProperty("max.batch.size", "1024");
        props.setProperty("max.queue.size", "4096");
        props.setProperty("max.wait.ms", "1000");
        props.setProperty("poll.interval.ms", "500");

        // 使用AsyncEmbeddedEngine并配置线程池
        props.setProperty("engine.type", "async");
        props.setProperty("num.stream.threads", "4");
        props.setProperty("task.threads", "2");

        return props;
    }

    @Override
    public void stop() {
        // 4. 注册关闭钩子，优雅退出
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                System.out.println("正在关闭Debezium引擎...");
                engine.close();
                executor.shutdown();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }));
    }

    @Override
    public void awaitStop() throws InterruptedException {
        engine.wait();
    }

    @Override
    public Collection<PluginConfigSpec<?>> configSchema() {
        // should return a list of all configuration options for this plugin
        return Arrays.asList(TOPIC_PREFIX, DATABASE_HOSTNAME, DATABASE_USER, DATABASE_PORT, DATABASE_PASSWORD,
                DATABASE_SERVER_ID, DATABASE_SERVER_NAME, DATABASE_INCLUDE_LIST, TABLE_INCLUDE_LIST, SNAPSHOT_MODE,
                OFFSET_FLUSH_INTERVAL_MS, OFFSET_STORAGE_FILE_FILENAME, INCLUDE_SCHEMA_CHANGES, ONLY_DDL);
    }

    @Override
    public String getId() {
        return this.id;
    }
}
