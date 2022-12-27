package io.asouquieres.kstream;

import io.asouquieres.data.*;
import io.asouquieres.kstream.helpers.AvroSerdes;
import io.asouquieres.kstream.helpers.StreamContext;

import io.asouquieres.kstream.stream.papi.reconciliation.PapiReconciliationConstants;
import io.asouquieres.kstream.stream.papi.reconciliation.PapiReconciliationTopology;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class PAPIReconciliationTopologyTest {
    private TestInputTopic<String, MainData> mainInputTopic;

    private TestInputTopic<String, SatelliteDataA> inputATopic;
    private TestInputTopic<String, SatelliteDataB> inputBTopic;
    private TestInputTopic<String, SatelliteDataC> inputCTopic;


    private TestOutputTopic<String, FullData> outputTopic;

    private TestOutputTopic<String, String> dlqTopic;

    private TopologyTestDriver testDriver;

    @BeforeEach
    public void init() {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(SCHEMA_REGISTRY_URL_CONFIG, "mock://dummy:1234");
        StreamContext.setProps(props);

        var topology = PapiReconciliationTopology.getTopology();

        testDriver = new TopologyTestDriver(topology, props);
        mainInputTopic = testDriver.createInputTopic(PapiReconciliationConstants.MAIN_DATA_TOPIC, Serdes.String().serializer(), AvroSerdes.<MainData>get().serializer());

        inputATopic = testDriver.createInputTopic(PapiReconciliationConstants.SATELLITE_INFO_A, Serdes.String().serializer(), AvroSerdes.<SatelliteDataA>get().serializer());
        inputBTopic = testDriver.createInputTopic(PapiReconciliationConstants.SATELLITE_INFO_B, Serdes.String().serializer(), AvroSerdes.<SatelliteDataB>get().serializer());
        inputCTopic = testDriver.createInputTopic(PapiReconciliationConstants.SATELLITE_INFO_C, Serdes.String().serializer(), AvroSerdes.<SatelliteDataC>get().serializer());

        outputTopic = testDriver.createOutputTopic(PapiReconciliationConstants.FULL_DATA_OUTPUT, Serdes.String().deserializer(), AvroSerdes.<FullData>get().deserializer());
        dlqTopic = testDriver.createOutputTopic(PapiReconciliationConstants.DLT, Serdes.String().deserializer(), Serdes.String().deserializer());

    }

    @AfterEach
    public void tearDown() {
        testDriver.close();
    }

    @Test
    public void shouldNotProduceOutputWithoutMainData() {

        inputATopic.pipeInput("b", SatelliteDataA.newBuilder().setDataId("corr1").setSatelliteInfoA("satA").build());
        inputBTopic.pipeInput("c", SatelliteDataB.newBuilder().setDataId("corr1").setSatelliteInfoB("satB").build());
        inputCTopic.pipeInput("d", SatelliteDataC.newBuilder().setDataId("corr1").setSatelliteInfoC("satC").build());

        var outputList = outputTopic.readValuesToList();
        var outputDlqList = dlqTopic.readValuesToList();


        // We expect no output result
        assertEquals(0, outputList.size());
        assertEquals(0, outputDlqList.size());
    }


    @Test
    public void shouldProduceWhenMainDataArrive() {
        var mainData = MainData.newBuilder()
                .setDataId("corr1")
                .setMainInfo1("m1")
                .setMainInfo2("m2")
                .build();

        inputATopic.pipeInput("b", SatelliteDataA.newBuilder().setDataId("corr1").setSatelliteInfoA("satA").build());
        inputBTopic.pipeInput("c", SatelliteDataB.newBuilder().setDataId("corr1").setSatelliteInfoB("satB").build());
        inputCTopic.pipeInput("d", SatelliteDataC.newBuilder().setDataId("corr1").setSatelliteInfoC("satC").build());
        mainInputTopic.pipeInput("a", mainData);

        var outputList = outputTopic.readValuesToList();
        var outputDlqList = dlqTopic.readValuesToList();


        // We expect no output result
        assertEquals(1, outputList.size());
        assertEquals(0, outputDlqList.size());
    }

    @Test
    public void shouldEnrichOverTimeWhenMainDataIsFirstEvent() {
        var mainData = MainData.newBuilder()
                .setDataId("corr1")
                .setMainInfo1("m1")
                .setMainInfo2("m2")
                .build();

        mainInputTopic.pipeInput("a", mainData);
        inputATopic.pipeInput("b", SatelliteDataA.newBuilder().setDataId("corr1").setSatelliteInfoA("satA").build());
        inputBTopic.pipeInput("c", SatelliteDataB.newBuilder().setDataId("corr1").setSatelliteInfoB("satB").build());
        inputCTopic.pipeInput("d", SatelliteDataC.newBuilder().setDataId("corr1").setSatelliteInfoC("satC").build());

        var outputList = outputTopic.readValuesToList();
        var outputDlqList = dlqTopic.readValuesToList();


        // We expect no output result
        assertEquals(4, outputList.size());
        assertEquals(0, outputDlqList.size());


        var expected = FullData.newBuilder()
                .setDataId("corr1")
                .setMainInfo1("m1")
                .setMainInfo2("m2")
                .build();

        assertEquals(expected, outputList.get(0));

        expected.setSatelliteInfo1("satA");
        assertEquals(expected, outputList.get(1));

        expected.setSatelliteInfo2("satB");
        assertEquals(expected, outputList.get(2));

        expected.setSatelliteInfo3("satC");
        assertEquals(expected, outputList.get(3));
    }
}
