package com.geekbang.bd.chenjd.hadooprpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;

import java.io.IOException;


/**
 * // 要求：输入你的真实学号，返回你的真实姓名
 * // 输入学号20210000000000，返回null
 * // 输入学号20210123456789，返回心心
 */
public class RPCServer {

    public static void main(String[] args) throws Exception {
        try {
            Server server = new RPC.Builder(new Configuration())
                    .setBindAddress("127.0.0.1") //绑定服务器地址
                    .setPort(8888) //绑定端口
                    .setProtocol(IRPCInterface.class)
                    .setInstance(new RPCInterfaceImpl())
                    .build();
            server.start();
        }catch(IOException exception) {
            System.out.println(exception.getMessage());
        }
    }
}
