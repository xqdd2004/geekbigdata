package com.dd.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 获取Hbase连接工厂类
 * 有改进空间
 * @author chenjd
 *
 */
public class HBaseConnectionFactory {

    private final static Logger logger =  LoggerFactory.getLogger(HBaseConnectionFactory.class);

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

        }catch (Exception ex){
            logger.error("连接Hbase失败，异常信息为：{}", ex.getMessage());
        }
    }

    public static  Connection getConnection(){
        if (connection==null || connection.isClosed()){
            try{
                connection = ConnectionFactory.createConnection(configuration);
            }catch (Exception ex){
                logger.error("连接Hbase失败，异常信息为：{}", ex.getMessage());
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
            } catch (IOException ex) {
                logger.error("关闭Hbase链接失败，异常信息为：{}", ex.getMessage());
            }
        }
    }

}

