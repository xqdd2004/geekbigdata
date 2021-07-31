package com.dd.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * 获取Hbase连接工厂类
 * 有改进空间
 * @author chenjd
 *
 */
public class HBaseConnectionFactory {

    private static final HBaseConnectionFactory factory = new HBaseConnectionFactory();
    //Hbase配置类
    private static  Configuration configuration;
    //Hbase连接类
    private static  Connection connection;

    private HBaseConnectionFactory(){
        try{
            if (configuration==null){
                configuration = HBaseConfiguration.create();
                configuration.set("hbase.zookeeper.quorum","hadoop0:2181");
            }

        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static  Connection getConnection(){
        if (connection==null || connection.isClosed()){
            try{
                connection = ConnectionFactory.createConnection(configuration);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        return connection;
    }

    /**
     * 关闭链接
     */
    public static void closeConn(){
        if (connection!=null){
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}

