package org.logstashplugins;

import co.elastic.logstash.api.Configuration;
import org.junit.Test;
import org.logstash.plugins.ConfigurationImpl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class InputDebeziumTest {

    @Test
    public void testJavaInputExample() throws InterruptedException {
        Map<String, Object> configValues = new HashMap<>();
        configValues.put(InputDebezium.DATABASE_HOSTNAME.name(), "127.0.0.1");
        configValues.put(InputDebezium.DATABASE_USER.name(), "root");
        configValues.put(InputDebezium.DATABASE_PASSWORD.name(), "admin");
        configValues.put(InputDebezium.DATABASE_INCLUDE_LIST.name(), "mydb");
        configValues.put(InputDebezium.TABLE_INCLUDE_LIST.name(), "mydb.t_user");
        Configuration config = new ConfigurationImpl(configValues);
        InputDebezium input = new InputDebezium("test-id", config, null);
        TestConsumer testConsumer = new TestConsumer();
        input.start(testConsumer);
        Thread.sleep(40000);
        List<Map<String, Object>> events = testConsumer.getEvents();
        for (int k = 1; k <= events.size(); k++) {
            System.out.println(events);
        }
    }

    private static class TestConsumer implements Consumer<Map<String, Object>> {

        private List<Map<String, Object>> events = new ArrayList<>();

        @Override
        public void accept(Map<String, Object> event) {
            synchronized (this) {
                events.add(event);
            }
        }

        public List<Map<String, Object>> getEvents() {
            return events;
        }
    }

}
