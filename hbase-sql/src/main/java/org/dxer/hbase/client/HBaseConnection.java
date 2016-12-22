package org.dxer.hbase.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

/**
 * @author linghf
 * @version 1.0
 * @class HBaseConnection
 * @since 2016年3月29日
 */
public class HBaseConnection {


    public static Connection getConnection() throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.10.101");
        Connection connection = ConnectionFactory.createConnection(conf);

        return connection;
    }
}
