package io.asouquieres.kstream;

import io.asouquieres.kstream.helpers.ContainerTestUtils;
import io.asouquieres.kstream.helpers.StreamContext;
import io.asouquieres.kstream.stream.simpleavro.SimpleStreamWithAvroTopology;
import io.asouquieres.data.MainData;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import net.christophschubert.cp.testcontainers.CPTestContainerFactory;
import net.christophschubert.cp.testcontainers.ConfluentServerContainer;
import net.christophschubert.cp.testcontainers.SchemaRegistryContainer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.lifecycle.Startable;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static io.asouquieres.kstream.stream.simpleavro.SimpleStreamWithAvroConstants.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class SimpleAvroTopologyIContainerTest {
    private Properties props;

    private  CPTestContainerFactory.ClusterSpec<ConfluentServerContainer> confluentServerCluster;
    private KafkaContainer firstKafkaContainer;
    private SchemaRegistryContainer schemaRegistryContainer;

    private AdminClient adminClient;

    private KafkaStreams streamApplication;

    // Before all the test in this class, startup a local containerized platform with Broker & Schema Registry
    // Initialize topics
    @BeforeEach
    public void init() {
        final CPTestContainerFactory factory = new CPTestContainerFactory();

        // Build local minimal platform
        confluentServerCluster = factory.withTag("7.3.1").createConfluentServerCluster(3);
        firstKafkaContainer = confluentServerCluster.kafkas.get(0);
        schemaRegistryContainer = factory.createSchemaRegistry(firstKafkaContainer);

        // Start the platform
        confluentServerCluster.startAll();
        schemaRegistryContainer.start();

        props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, confluentServerCluster.getBootstrap());


        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-id");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);

        // If this one is not present, special avro logical type will not be applied, a timestamp will become a Long
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryContainer.getBaseUrl());

        var input = new NewTopic(SOURCE_TOPIC, 3, (short) 1);
        var output = new NewTopic(FILTERED_TOPIC, 3, (short) 1);
        var dlq = new NewTopic(DLT, 3, (short) 1);

        adminClient = AdminClient.create(props);
        adminClient.createTopics(Arrays.asList(input,output,dlq));

        StreamContext.setProps(props);

        var topology = SimpleStreamWithAvroTopology.getTopology();

        streamApplication = new KafkaStreams(topology, props);
        streamApplication.start();

    }

    @BeforeEach
    public void beforeEachTest() {
        // Reset stream application
        // Start the stream application

    }

    @AfterEach
    public void tearDown() {
        confluentServerCluster.kafkas.forEach(Startable::close);
        schemaRegistryContainer.close();
    }

    @Test
    public void shouldProduceALlInputValuesAndADlqMessage() throws ExecutionException, InterruptedException {

        var c1 = MainData.newBuilder()
                .setDataId("1")
                .setMainInfo1("name")
                .setMainInfo2("63600")
                .build();


        ContainerTestUtils.produceKeyValuesSynchronously(SOURCE_TOPIC, Arrays.asList(KeyValue.pair("key1",c1)), props);

        List<KeyValue<String,MainData>> result = ContainerTestUtils.waitUntilMinKeyValueRecordsReceived(props,FILTERED_TOPIC, 1, 30000);

        assertEquals("key1", result.get(0).key);
        assertEquals(c1, result.get(0).value);
    }
}
