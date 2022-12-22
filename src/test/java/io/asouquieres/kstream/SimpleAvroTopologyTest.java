package io.asouquieres.kstream;

import io.asouquieres.kstream.helpers.AvroSerdes;
import io.asouquieres.kstream.helpers.StreamContext;
import io.asouquieres.kstream.stream.simpleavro.SimpleStreamWithAvroConstants;
import io.asouquieres.kstream.stream.simpleavro.SimpleStreamWithAvroTopology;
import io.asouquieres.data.MainData;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class SimpleAvroTopologyTest {
    private TestInputTopic<String, MainData> inputTopic;
    private TestOutputTopic<String, MainData> outputTopic;

    private TestOutputTopic<String, String> dlqTopic;

    private TopologyTestDriver testDriver;

    @BeforeEach
    public void init() {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(SCHEMA_REGISTRY_URL_CONFIG, "mock://dummy:1234");
        StreamContext.setProps(props);

        var topology = SimpleStreamWithAvroTopology.getTopology();

        testDriver = new TopologyTestDriver(topology, props);
        inputTopic = testDriver.createInputTopic(SimpleStreamWithAvroConstants.SOURCE_TOPIC, Serdes.String().serializer(), AvroSerdes.<MainData>get().serializer());
        outputTopic = testDriver.createOutputTopic(SimpleStreamWithAvroConstants.FILTERED_TOPIC, Serdes.String().deserializer(), AvroSerdes.<MainData>get().deserializer());

        dlqTopic = testDriver.createOutputTopic(SimpleStreamWithAvroConstants.DLT, Serdes.String().deserializer(), Serdes.String().deserializer());

    }

    @AfterEach
    public void tearDown() {
        testDriver.close();
    }

    @Test
    public void shouldProduceALlInputValuesAndADlqMessage() {

        var c1 = MainData.newBuilder()
                .setDataId("1")
                .setMainInfo1("name")
                .setMainInfo2("63600")
                .build();

        inputTopic.pipeInput("key1", c1);
        inputTopic.pipeInput("key1", c1);
        inputTopic.pipeInput("key1", c1);
        inputTopic.pipeInput("key1", c1);
        inputTopic.pipeInput("key1", c1);

        var outputList = outputTopic.readValuesToList();
        var outputDlqList = dlqTopic.readValuesToList();


        assertEquals(0, outputDlqList.size());
        assertEquals(c1, outputList.get(0));
    }
}
