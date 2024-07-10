package io.asouquieres.kstream.reconciliation.papi;

import io.asouquieres.kstream.helpers.PropertiesLoader;
import io.asouquieres.kstream.helpers.StreamContext;
import io.asouquieres.kstream.reconciliation.ReconciliationConstants;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.concurrent.CountDownLatch;

import static io.asouquieres.kstream.helpers.StreamExceptionCatcher.DLQ_NAME;

@SpringBootApplication
public class PapiReconciliationLauncherApp implements CommandLineRunner {

    Logger logger = LoggerFactory.getLogger(PapiReconciliationLauncherApp.class);
    private final ConfigurableApplicationContext applicationContext;

    public PapiReconciliationLauncherApp(MeterRegistry meterRegistry, ConfigurableApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    public static void main(String[] args) {
        SpringApplication.run(PapiReconciliationLauncherApp.class, args);
    }

    @Override
    public void run(String... args) throws Exception {

        // Get stream configuration from resource folder
        var streamsConfiguration = PropertiesLoader.fromYaml("application.yml");

        StreamContext.setProps(streamsConfiguration);
        var p = StreamContext.getProps();

        p.setProperty(DLQ_NAME, ReconciliationConstants.Topics.DLT);
        // Build topology
        try (var stream = new KafkaStreams(PapiReconciliationTopology.getTopology(), streamsConfiguration)) {

            // Define handler in case of unmanaged exception
            stream.setUncaughtExceptionHandler( e -> {
                logger.error("Uncaught exception occurred in Kafka Streams. Application will shutdown !", e);

                // Consider REPLACE_THREAD if the exception is retriable
                // Consider SHUTDOWN_APPLICATION if the exception may propagate to other instances after rebalance
                return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            });

            // Hoook the main application to Kafka Stream Lifecycle, to avoid zombie stream application
            stream.setStateListener(((newState, oldState) -> {
                if (newState == KafkaStreams.State.PENDING_ERROR) {
                    //Stop the app in case of error
                    if (applicationContext.isActive()) {
                        SpringApplication.exit(applicationContext, () -> 1);
                    }
                }
            }));

            // Start stream execution
            stream.start();

            // Ensure your app respond gracefully to external shutdown signal
            final CountDownLatch latch = new CountDownLatch(1);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                stream.close();
                latch.countDown();
            }));

            latch.await();
        }
    }
}
