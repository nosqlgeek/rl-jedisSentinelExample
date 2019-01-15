package redis.clients.jedis;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;

public class JedisSentinelPoolPatched extends JedisPoolAbstract {

    protected GenericObjectPoolConfig poolConfig;

    protected int connectionTimeout = Protocol.DEFAULT_TIMEOUT;
    protected int soTimeout = Protocol.DEFAULT_TIMEOUT;

    protected String password;

    protected int database = Protocol.DEFAULT_DATABASE;

    protected String clientName;

    protected Set<MasterListener> masterListeners = new HashSet<MasterListener>();

    protected Logger log = LoggerFactory.getLogger(getClass().getName());

    private volatile JedisFactory factory;
    private volatile HostAndPort currentHostMaster;

    public JedisSentinelPoolPatched(String masterName, Set<String> sentinels,
                                    final GenericObjectPoolConfig poolConfig) {
        this(masterName, sentinels, poolConfig, Protocol.DEFAULT_TIMEOUT, null,
                Protocol.DEFAULT_DATABASE);
    }

    public JedisSentinelPoolPatched(String masterName, Set<String> sentinels) {
        this(masterName, sentinels, new GenericObjectPoolConfig(), Protocol.DEFAULT_TIMEOUT, null,
                Protocol.DEFAULT_DATABASE);
    }

    public JedisSentinelPoolPatched(String masterName, Set<String> sentinels, String password) {
        this(masterName, sentinels, new GenericObjectPoolConfig(), Protocol.DEFAULT_TIMEOUT, password);
    }

    public JedisSentinelPoolPatched(String masterName, Set<String> sentinels,
                                    final GenericObjectPoolConfig poolConfig, int timeout, final String password) {
        this(masterName, sentinels, poolConfig, timeout, password, Protocol.DEFAULT_DATABASE);
    }

    public JedisSentinelPoolPatched(String masterName, Set<String> sentinels,
                                    final GenericObjectPoolConfig poolConfig, final int timeout) {
        this(masterName, sentinels, poolConfig, timeout, null, Protocol.DEFAULT_DATABASE);
    }

    public JedisSentinelPoolPatched(String masterName, Set<String> sentinels,
                                    final GenericObjectPoolConfig poolConfig, final String password) {
        this(masterName, sentinels, poolConfig, Protocol.DEFAULT_TIMEOUT, password);
    }

    public JedisSentinelPoolPatched(String masterName, Set<String> sentinels,
                                    final GenericObjectPoolConfig poolConfig, int timeout, final String password,
                                    final int database) {
        this(masterName, sentinels, poolConfig, timeout, timeout, password, database);
    }

    public JedisSentinelPoolPatched(String masterName, Set<String> sentinels,
                                    final GenericObjectPoolConfig poolConfig, int timeout, final String password,
                                    final int database, final String clientName) {
        this(masterName, sentinels, poolConfig, timeout, timeout, password, database, clientName);
    }

    public JedisSentinelPoolPatched(String masterName, Set<String> sentinels,
                                    final GenericObjectPoolConfig poolConfig, final int timeout, final int soTimeout,
                                    final String password, final int database) {
        this(masterName, sentinels, poolConfig, timeout, soTimeout, password, database, null);
    }

    public JedisSentinelPoolPatched(String masterName, Set<String> sentinels,
                                    final GenericObjectPoolConfig poolConfig, final int connectionTimeout, final int soTimeout,
                                    final String password, final int database, final String clientName) {
        this.poolConfig = poolConfig;
        this.connectionTimeout = connectionTimeout;
        this.soTimeout = soTimeout;
        this.password = password;
        this.database = database;
        this.clientName = clientName;

        log.debug("Patched version of JedisSentinelPool; Author: david.maier@redislabs.com; Date: Tue 15 Jan 2019");

        HostAndPort master = initSentinels(sentinels, masterName);
        initPool(master);
    }

    @Override
    public void destroy() {
        for (MasterListener m : masterListeners) {
            m.shutdown();
        }

        super.destroy();
    }

    public HostAndPort getCurrentHostMaster() {
        return currentHostMaster;
    }

    private void initPool(HostAndPort master) {

        if (!master.equals(currentHostMaster)) {

            log.debug("The master changed since the last initialization. Re-initializing the factory and the pool ...");

            currentHostMaster = master;

            if (factory == null) {
                factory = new JedisFactory(master.getHost(), master.getPort(), connectionTimeout,
                        soTimeout, password, database, clientName);
                initPool(poolConfig, factory);
            } else {

                log.debug("The current master is the same one as the previous one. Reconfiguring the factory and " +
                          "clearing the pool ...");

                factory.setHostAndPort(currentHostMaster);
                // although we clear the pool, we still have to check the
                // returned object
                // in getResource, this call only clears idle instances, not
                // borrowed instances
                internalPool.clear();
            }

            log.info("Created JedisPool to master at " + master);
        }
    }

    private HostAndPort initSentinels(Set<String> sentinels, final String masterName) {

        HostAndPort master = null;
        boolean sentinelAvailable = false;

        log.info("Trying to find master from available Sentinels...");

        for (String sentinel : sentinels) {
            final HostAndPort hap = HostAndPort.parseString(sentinel);

            log.debug("Connecting to Sentinel {}", hap);

            Jedis jedis = null;
            try {
                jedis = new Jedis(hap);

                List<String> masterAddr = jedis.sentinelGetMasterAddrByName(masterName);

                // connected to sentinel...
                sentinelAvailable = true;

                if (masterAddr == null || masterAddr.size() != 2) {
                    log.warn("Can not get master addr, master name: {}. Sentinel: {}", masterName, hap);
                    continue;
                }

                master = toHostAndPort(masterAddr);
                log.debug("Found Redis master at {}", master);
                break;
            } catch (JedisException e) {
                // resolves #1036, it should handle JedisException there's another chance
                // of raising JedisDataException
                log.warn(
                        "Cannot get master address from sentinel running @ {}. Reason: {}. Trying next one.", hap,
                        e.toString());
            } finally {
                if (jedis != null) {
                    jedis.close();
                }
            }
        }

        if (master == null) {
            if (sentinelAvailable) {
                // can connect to sentinel, but master name seems to not
                // monitored
                throw new JedisException("Can connect to sentinel, but " + masterName
                        + " seems to be not monitored...");
            } else {
                throw new JedisConnectionException("All sentinels down, cannot determine where is "
                        + masterName + " master is running...");
            }
        }

        log.info("Redis master running at " + master + ", starting Sentinel listeners...");

        for (String sentinel : sentinels) {
            final HostAndPort hap = HostAndPort.parseString(sentinel);
            MasterListener masterListener = new MasterListener(masterName, hap.getHost(), hap.getPort());
            // whether MasterListener threads are alive or not, process can be stopped
            masterListener.setDaemon(true);
            masterListeners.add(masterListener);
            masterListener.start();
        }

        return master;
    }

    private HostAndPort toHostAndPort(List<String> getMasterAddrByNameResult) {
        String host = getMasterAddrByNameResult.get(0);
        int port = Integer.parseInt(getMasterAddrByNameResult.get(1));

        return new HostAndPort(host, port);
    }

    @Override
    public Jedis getResource() {
        while (true) {

            log.debug("Getting resource ...");
            Jedis jedis = super.getResource();
            log.debug("New resource retrieved.");
            jedis.setDataSource(this);


            // get a reference because it can change concurrently
            final HostAndPort master = currentHostMaster;


            log.debug("master host = {}", master.getHost());
            log.debug("master port = {}", master.getPort());
            log.debug("pool host = {}", jedis.getClient().getHost());
            log.debug("pool port = {}", jedis.getClient().getPort());

            final HostAndPort connection = new HostAndPort(jedis.getClient().getHost(), jedis.getClient()
                    .getPort());

            if (master.equals(connection)) {
                // connected to the correct master
                log.debug("Connected to the right master");
                return jedis;
            } else {
                log.debug("Pool returned invalid connection");
                returnBrokenResource(jedis);
            }
        }
    }

    @Override
    protected void returnBrokenResource(final Jedis resource) {
        if (resource != null) {
            returnBrokenResourceObject(resource);
        }
    }

    @Override
    protected void returnResource(final Jedis resource) {
        if (resource != null) {
            resource.resetState();
            returnResourceObject(resource);
        }
    }

    protected class MasterListener extends Thread {

        protected String masterName;
        protected String host;
        protected int port;

        //Base timout
        protected long subscribeRetryWaitTimeMillis = 1000;

        //Wait factor
        protected  int subscribeRetryFactor = 2;

        //Max. number of retries
        protected  int subscribeMaxRetries = 8;

        protected volatile Jedis sentJ;
        protected AtomicBoolean running = new AtomicBoolean(false);

        protected MasterListener() {
        }

        public MasterListener(String masterName, String host, int port) {
            super(String.format("MasterListener-%s-[%s:%d]", masterName, host, port));
            this.masterName = masterName;
            this.host = host;
            this.port = port;
        }

        public MasterListener(String masterName, String host, int port,
                              long subscribeRetryWaitTimeMillis) {
            this(masterName, host, port);
            this.subscribeRetryWaitTimeMillis = subscribeRetryWaitTimeMillis;
        }

        /**
         * Wait exponentially for the Sentinel to become available again
         *
         * Why doesn't it hurt to wait longer than the previous value (5sec)?
         *
         * 1. If there is just 1 Sentinel then we give this single Sentinel a chance to come back
         * (i.e. if there is a DNS failover) The pool will be re-initialized as soon as the connection to the Sentinel
         * can be re-established
         *
         * 2. If we have multiple Sentinels configured and this one here should not come back in time,  then another
         * MasterListener will realize (via its subscribtion to the channel +switch-master) that the master changed
         * which will cause a re-initialization of the pool anyway.
         *
         * @param sentinelHost
         * @param sentinelPort
         * @throws InterruptedException
         */
        private void waitForSentinel(String sentinelHost, int sentinelPort) throws InterruptedException {

            int numErr = 0;
            boolean isWaiting = true;
            long currWait = subscribeRetryWaitTimeMillis;

            while (isWaiting)

                try {

                    log.debug("Trying to connect to Sentinel ...");
                    log.debug("host = {}", sentinelHost);
                    log.debug("port = {}", sentinelPort);

                    Jedis j = new Jedis(sentinelHost, sentinelPort, 500);
                    j.connect();

                    log.debug("status = " + j.isConnected());
                    
                    isWaiting = false;

                    log.debug("Successfully reconnected to Sentinel.");

                } catch (JedisConnectionException e) {

                    numErr++;
                    log.debug("Waiting in cycle {} for {} ms ...", numErr, currWait);
                    Thread.sleep(currWait);
                    currWait=currWait*subscribeRetryFactor;
                    if (numErr == subscribeMaxRetries) isWaiting = false;

                }
        }





        @Override
        public void run() {

            running.set(true);

            while (running.get()) {

                log.debug("Connecting to Sentinel {}:{} ...", host, port);
                sentJ = new Jedis(host, port);

                try {
                    // double check that it is not being shutdown
                    if (!running.get()) {
                        break;
                    }

                    /*
                     * Added code for active refresh
                     */
                    List<String> masterAddr = sentJ.sentinelGetMasterAddrByName(masterName);

                    if (masterAddr == null || masterAddr.size() != 2) {
                        log.warn("Can not get master addr, master name: {}. Sentinel: {}：{}.",masterName,host,port);
                    }else{

                        initPool(toHostAndPort(masterAddr));
                    }

                    sentJ.subscribe(new JedisPubSub() {
                        @Override
                        public void onMessage(String channel, String message) {
                            log.debug("Sentinel {}:{} published: {}.", host, port, message);

                            String[] switchMasterMsg = message.split(" ");

                            if (switchMasterMsg.length > 3) {

                                if (masterName.equals(switchMasterMsg[0])) {
                                    initPool(toHostAndPort(Arrays.asList(switchMasterMsg[3], switchMasterMsg[4])));
                                } else {
                                    log.debug(
                                            "Ignoring message on +switch-master for master name {}, our master name is {}",
                                            switchMasterMsg[0], masterName);
                                }

                            } else {
                                log.error(
                                        "Invalid message received on Sentinel {}:{} on channel +switch-master: {}", host,
                                        port, message);
                            }
                        }
                    }, "+switch-master");

                } catch (JedisException e) {

                    if (running.get()) {

                        log.error("Lost connection to Sentinel at {}:{}. Retrying a few times ...", host,
                                port, e);
                        try {

                            waitForSentinel(host, port);

                        } catch (InterruptedException e1) {
                            log.error("Sleep interrupted: ", e1);
                        }
                    } else {
                        log.debug("Unsubscribing from Sentinel at {}:{}", host, port);
                    }
                } finally {
                    sentJ.close();
                }
            }
        }

        public void shutdown() {
            try {
                log.debug("Shutting down listener on {}:{}", host, port);
                running.set(false);
                // This isn't good, the Jedis object is not thread safe
                if (sentJ != null) {
                    sentJ.disconnect();
                }
            } catch (Exception e) {
                log.error("Caught exception while shutting down: ", e);
            }
        }
    }
}
