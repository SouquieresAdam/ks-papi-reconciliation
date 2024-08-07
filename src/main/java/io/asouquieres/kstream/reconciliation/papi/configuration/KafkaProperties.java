package io.asouquieres.kstream.reconciliation.papi.configuration;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * The Kafka properties class.
 */
@Configuration
@ConfigurationProperties(prefix = "kafka")
public class KafkaProperties {
    /**
     * The Kafka properties.
     */
    private Map<String, String> properties = new HashMap<>();

    /**
     * Return the Kafka properties as {@link java.util.Properties}.
     *
     * @return The Kafka properties
     */
    public Properties asProperties() {
        Properties props = new Properties();
        props.putAll(properties);
        return props;
    }
}