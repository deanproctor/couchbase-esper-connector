package example;

import com.couchbase.client.deps.com.fasterxml.jackson.core.JsonProcessingException;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.java.document.RawJsonDocument;
import com.couchbase.esper.*;
import com.espertech.esper.client.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Random;

/**
 * Created by David on 03/02/2015.
 */
public class Main {

    private static Random random = new Random();
    private static final Log log = LogFactory.getLog(Main.class);

    public static void main(String[] args) {
        Main sample = new Main();
        try {
            sample.run();
        } catch (RuntimeException ex) {
            log.error("Unexpected exception :" + ex.getMessage(), ex);
        }
    }

    public void run()
    {
        log.info("Setting up engine instance.");
        Configuration config = new Configuration();
        config.addEventType(Data.class);
        config.addPlugInVirtualDataWindow("couchbase", "couchbasevdw", CouchbaseVirtualDataWindowFactory.class.getName());
        final EPServiceProvider epService = EPServiceProviderManager.getProvider("CouchbaseExternalDataExample", config);

        epService.getEPAdministrator().createEPL("create schema CouchbaseEvent as (key string, value example.Data)");

        log.info("Creating named window with virtual.");

        epService.getEPAdministrator().createEPL("create window CouchbaseWindow.couchbase:couchbasevdw() as CouchbaseEvent");

        for(int numThreads = 0; numThreads < 128; numThreads++) {
            new Thread() {
                public void run() {
                    runSampleFireAndForgetQuery(epService);
                }
            }.start();
        }
    }

    private void runSampleFireAndForgetQuery(EPServiceProvider epService) {
        while(true) {
            String fireAndForget = "select * from CouchbaseWindow where key in ('test1','test2','test3','test4','test5','test6','test7','test8','test9','test10','test11','test12','test13','test14','test15','test16','test17','test18','test19','test20','test21','test22','test23','test24','test25','test26','test27','test28','test29','test30','test31','test32','test33','test34','test35','test36','test37','test38','test39','test40','test41','test42','test43','test44','test45','test46','test47','test48','test49','test50','test51','test52','test53','test54','test55','test56','test57','test58','test59','test60','test61','test62','test63','test64','test65','test66','test67','test68','test69','test70','test71','test72','test73','test74','test75','test76','test77','test78','test79','test80','test81','test82','test83','test84','test85','test86','test87','test88','test89','test90','test91','test92','test93','test94','test95','test96','test97','test98','test99','test100')";
            EPOnDemandQueryResult result = epService.getEPRuntime().executeQuery(fireAndForget);
            log.info("Fire-and-forget query returned: ");
            for (EventBean eventBean : result.getArray()) {
                System.out.println(eventBean.get("key") + " : " + eventBean.get("value").toString());
            }
        }
    }
}


