package io.asouquieres.kstream.helpers;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyDescription;

import java.util.stream.Collectors;

public class TopologySizer {


    final static int GLOBAL_STORE_MEMORY = 100;
    final static int NON_GLOBAL_STORE_MEMORY = 100;
    final static int SUBTOPOLOGY_HEAP_MEMORY = 82;
    final static int DEPENDENCIES_HEAP_MEMORY = 200;

    public static void printSizing(StreamsBuilder sb) {
        var topo = sb.build();
        var nbGlobalStore = topo.describe().globalStores().size();
        var nbNonGlobalStore = topo.describe().subtopologies().stream()
                .flatMap(subtopology -> subtopology.nodes().stream())
                .filter(n -> n instanceof TopologyDescription.Processor)
                .flatMap(p -> ((TopologyDescription.Processor) p).stores().stream())
                .collect(Collectors.toSet()).size();
        var nbSubTopology = topo.describe().subtopologies().size();


        var fixedNonHeapMemory = (nbGlobalStore * GLOBAL_STORE_MEMORY);
        var variableNonHeapMemory = nbNonGlobalStore * NON_GLOBAL_STORE_MEMORY;
        var variableHeapMemory = nbSubTopology * SUBTOPOLOGY_HEAP_MEMORY;
        long baseMemory = fixedNonHeapMemory + variableNonHeapMemory + variableHeapMemory + DEPENDENCIES_HEAP_MEMORY;
        long additionalPerPartition = variableNonHeapMemory + variableHeapMemory;

        System.out.println("----------------------------------------------------------------");
        System.out.println("----------------------------------------------------------------");
        System.out.println("----------------------------------------------------------------");

        System.out.println("MEMORY REQUIREMENTS:");
        System.out.println("- Fixed Non-heap Memory: " + fixedNonHeapMemory + " MB");
        System.out.println("- Variable Non-heap Memory: " + variableNonHeapMemory + " MB");
        System.out.println("- Variable Heap Memory: " + variableHeapMemory + " MB");
        System.out.println("Total Base Memory Required: " + baseMemory + " MB");
        System.out.println();
        System.out.println("Additionally, each extra partition may require approximately "
                + additionalPerPartition + " MB.");

        // Provide a note for users to adjust parameters as needed
        System.out.println("\nNote: These estimates assume default RocksDB configurations. Adjust memory allocations based on your specific workload and tuning requirements.");
        System.out.println("----------------------------------------------------------------");
        System.out.println("----------------------------------------------------------------");
        System.out.println("----------------------------------------------------------------");
    }
}
