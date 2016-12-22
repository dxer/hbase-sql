package org.dxer.hbase.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author linghf
 * @version 1.0
 * @class HBaseConfig
 * @since 2016年3月29日
 */
public class HBaseConfig {

    private static Configuration configuration = null;

    private static Map<String, String> config = new HashMap<String, String>();

    private static Properties prop = new Properties();

    static {
        try {
            InputStream is = HBaseConfig.class.getResourceAsStream("../../../../hbase.properties");
            prop.load(is);
            if (is != null) {
                is.close();
            }
        } catch (Exception e) {
            System.err.println("file " + "hbase.properties" + " not found!\n" + e);
        }
    }

    public static String getValue(String key) {
        String value = prop.getProperty(key);

        return value;
    }

    public static void set(String name, String value) {
        if (name != null && name.length() > 0) {
            config.put(name, value);
        }
    }

    public static boolean isKerberos() {
        String value = getValue("hbase.kerberos");
        if (value != null && value.length() > 0) {
            return "true".equals(value.toLowerCase());
        }
        return false;
    }

    public static Configuration getConfiguration() {
        if (configuration == null) {
            configuration = HBaseConfiguration.create();
        }

        System.out.println(getValue("hbase.zookeeper.quorum"));
        configuration.set("hbase.zookeeper.quorum", getValue("hbase.zookeeper.quorum"));

        if (isKerberos()) {
            configuration.set("hadoop.security.authorization", "true");
            configuration.set("hadoop.security.authentication", "kerberos");
            configuration.set("hbase.rpc.engine", "org.apache.hadoop.hbase.ipc.SecureRpcEngine");
            configuration.set("hbase.master.kerberos.principal", "hbase/_HOST@E.189.CN");
            configuration.set("hbase.regionserver.kerberos.principal", "hbase/_HOST@E.189.CN");
            configuration.set("hbase.security.authorization=", "true");
            configuration.set("hbase.security.authentication", "kerberos");
        }

//        configuration.set("hbase.zookeeper.property.clientPort", getValue("hbase.zookeeper.property.clientPort"));
//        configuration.set("hbase.hstore.flusher.count", getValue("hbase.hstore.flusher.count")); // 1
//        configuration.set("hbase.client.write.buffer", getValue("hbase.client.write.buffer")); // 2m
//
//        configuration.set("hbase.client.retries.number", getValue("hbase.client.retries.number"));
//        configuration.set("hbase.client.pause", getValue("hbase.client.pause"));
//
//        configuration.set("zookeeper.recovery.retry", getValue("zookeeper.recovery.retry"));
//        configuration.set("zookeeper.recovery.retry.intervalmill", getValue("zookeeper.recovery.retry.intervalmill"));

        if (config != null && !config.isEmpty()) {
            for (String name : config.keySet()) {
                String value = config.get(name);
                configuration.set(name, value);
            }
        }

        return configuration;
    }

}
