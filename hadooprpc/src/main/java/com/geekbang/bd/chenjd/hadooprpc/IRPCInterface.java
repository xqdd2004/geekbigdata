package com.geekbang.bd.chenjd.hadooprpc;

/**
 * 接口类，定义一个获取姓名的接口
 */
public interface IRPCInterface {
    public static final long versionID = 1;
    public String findName(String studentId);
}
