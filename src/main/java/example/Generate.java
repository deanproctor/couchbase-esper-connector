package example;

import com.couchbase.client.deps.com.fasterxml.jackson.core.JsonProcessingException;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.RawJsonDocument;

import java.io.IOException;

/**
 * Created by David on 04/02/2015.
 */
public class Generate {
    public static void main(String[] args) {
        ObjectMapper mapper = new ObjectMapper();
        String json = null;
        try {
            json = mapper.writeValueAsString(new Data("hello", 13.12));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

       final String value = json;

        Cluster cluster = CouchbaseCluster.create("10.253.128.174");
        final Bucket bucket = cluster.openBucket("esper");
        new Thread() {
            public void run() {
                for(int i = 0; i < 25000; i++) {
                    bucket.upsert(RawJsonDocument.create("test" + i, value));
                }
            }
        }.start();
        new Thread() {
            public void run() {
                for(int i = 25000; i < 50000; i++) {
                    bucket.upsert(RawJsonDocument.create("test" + i, value));
                }
            }
        }.start();
        new Thread() {
            public void run() {
                for(int i = 50000; i < 75000; i++) {
                    bucket.upsert(RawJsonDocument.create("test" + i, value));
                }
            }
        }.start();
        new Thread() {
            public void run() {
                for(int i = 75000; i < 100000; i++) {
                    bucket.upsert(RawJsonDocument.create("test" + i, value));
                }
            }
        }.start();

    }
}