package com.dd.hbase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HBaseClient {
    private final static Logger logger =  LoggerFactory.getLogger(HBaseClient.class);

    public static void main(String[] args) {
        logger.error("err test2");
        logger.warn("warn test");
    }
}
