package io.asouquieres.kstream.reconciliation;

public interface ReconciliationConstants {

    interface Topics {
        String MAIN_DATA_TOPIC = "MainData";
        String SATELLITE_INFO_A = "SatelliteA";
        String SATELLITE_INFO_B = "SatelliteB";
        String SATELLITE_INFO_C = "SatelliteC";

        String FULL_DATA_OUTPUT = "FullData_Output";

        String DLT = "Reconciliation_DLT";
    }

    interface Statestores {
        String RECONCILIATION_STORE = "Reconciliation-Store";
    }

    interface Repartitions {
        String MAIN_DATA_BY_CORRELATION_KEY = "MainData_byDataId";
        String SATELLITEDATAA_BY_CORRELATION_KEY ="SatelliteDataA_byDataId";
        String SATELLITEDATAB_BY_CORRELATION_KEY = "SatelliteDataB_byDataId";
        String SATELLITEDATAC_BY_CORRELATION_KEY = "SatelliteDataC_byDataId";
    }
}
