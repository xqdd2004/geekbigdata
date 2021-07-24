package com.geekbang.bd.chenjd.hadooprpc;

import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

/**
 * RPC 客户端程序
 * 要求：输入你的真实学号，返回你的真实姓名
 * 输入学号20210000000000，返回null
 * 输入学号20210123456789，返回心心
 *
 */
public class RPCClient {

    public static void main(String[]args) throws Exception {
        IRPCInterface proxy = RPC.getProxy(IRPCInterface.class, 1,
                new InetSocketAddress("127.0.0.1", 8888), new Configuration());

        //获取我的名字
        String myName = proxy.findName("G20200389040009");
        System.out.println("my Name is " + myName);

        //获取心心的名字
        String xinName = proxy.findName("G20210123456789");
        System.out.println("xinxin's Name is " + xinName);

        //获取路人的名字
        String otherName = proxy.findName("G20210000000000");
        System.out.println("Other's Name is " + otherName);
    }

}