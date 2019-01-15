package com.redislabs.test;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.*;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisSentinelPoolPatched;
import redis.clients.jedis.exceptions.JedisConnectionException;
import java.util.HashSet;
import java.util.Set;


/**
 * A test case for double checking how the JedisSentinelPool implementation behaves in case of a node failover and
 * a single but highly available Sentinel Discovery service endpoint.
 *
 * Further details about the Redis Enterprise discovery service can be found here:
 *
 * - https://docs.redislabs.com/latest/rs/concepts/data-access/discovery-service/
 */
public class Main {

    public static final Logger log = LoggerFactory.getLogger("test");

    public static final String MASTER_NAME = "demo@internal";
    public static final String DB_ENDPOINT = "redis-16379.internal.cluster.ubuntu-docker.org";
    public static final int SENT_PORT=8001;
    public static final String SENTINEL = DB_ENDPOINT + ":" + SENT_PORT;

    public static final int TIMEOUT_BASE = 2000;
    public static final int TIMEOUT_FACTOR = 2;
    public  static final int TIMEOUT_MAXRETRIES = 8;


    /**
     * Exponential wait
     *
     * Wait for TIMEOUT=TIMEOUT_BASE, then wait for TIMEOUT=TIMEOUT*TIMEOUT_FACTOR for a maximum number of retries of
     * TIMEOUT_MAXRETRIES
     */
    public static void waitForConnection(JedisSentinelPoolPatched sentinelPool) throws InterruptedException {

        int numErr = 0;
        boolean isWaiting = true;
        long currWait = TIMEOUT_BASE;


        log.info("-- Failover start");

        while (isWaiting) {

            try {

                log.info("Trying to connect to the master {} ...", sentinelPool.getCurrentHostMaster());
                Jedis j = new Jedis(sentinelPool.getCurrentHostMaster().getHost(), sentinelPool.getCurrentHostMaster().getPort(), 500);
                j.connect();
                isWaiting = false;
                log.info("Successfully reconnected to the master {} ...", sentinelPool.getCurrentHostMaster());

            } catch (JedisConnectionException e) {

                numErr++;

                log.info("Waiting in cycle {} for {} ms ...", numErr, currWait);
                Thread.sleep(currWait);

                currWait = currWait * TIMEOUT_FACTOR;
                if (numErr == TIMEOUT_MAXRETRIES) isWaiting = false;
            }
        }

        log.info("-- Failover end");
    }


    /**
     * Demonstrate the reconnect in case of a failure
     *
     * @param args
     * @throws InterruptedException
     */
    public static void main(String[] args) throws InterruptedException {

        log.info("Connecting ...");

        /*
        We are passing a single Sentinel here by expecting that the Sentinel service is available under the same name
        after the failover of a node is completed
        */
        Set<String> sentinels = new HashSet<String>();
        sentinels.add(SENTINEL);

        //GenericObjectPoolConfig cfg = new GenericObjectPoolConfig();
        //JedisSentinelPoolPatched sentinelPool = new JedisSentinelPoolPatched(MASTER_NAME, sentinels, cfg, 60000 );

        /*
         * The JedisSentinelPool is using a timeout value of 5sec for reconnecting to one of the Sentinels.
         * Our patched version is using an exponential wait in order to give the Sentinel endpoint a chance to get
         * failed over.
         */
        JedisSentinelPoolPatched sentinelPool = new JedisSentinelPoolPatched(MASTER_NAME, sentinels);
        log.info("master: " + sentinelPool.getCurrentHostMaster());


        /*
        This is the connection which you got from the pool. There is no automatic reconnect but a an Exception when this
        connection drops. So you will need to request a new connection from the pool in case of a disconnect.
         */
        Jedis redis = sentinelPool.getResource();

        log.info("Setting test value ...");
        redis.set("hello", "world");


        log.info("Starting to fetch the test value again and again ...");

        while (true) {

            try {

                String result = redis.get("hello");

                //Skip output if we get the expected result
                if (!result.equals("world")) log.warn("Expected 'hello' but got {}", result);

            } catch (JedisConnectionException ex) {

                log.warn("Client was disconnected, trying to reconnect ...");

                /*
                You need to wait until the failover is completed.
                The failover time in a Cloud deployment is expected to be slightly higher than for an on-premise
                installation as it is recommended to use a different watchdog profile in order to avoid false-positives
                due to bad network conditions.
                */
                waitForConnection(sentinelPool);

                log.info("Requesting a new connection to {} ...", sentinelPool.getCurrentHostMaster());
                redis = sentinelPool.getResource();

            }
        }
    }


}
