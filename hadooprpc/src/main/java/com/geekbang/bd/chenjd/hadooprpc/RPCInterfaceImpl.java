package com.geekbang.bd.chenjd.hadooprpc;


/**
 * 接口实现类
 * 目前实现很简单，也可扩展为从数据库或map等获取
 *
 */
public class RPCInterfaceImpl implements IRPCInterface {

    //G20200389040009
    public String findName(String studentId) {
       String studentName = null;
        if(studentId.equals("G20200389040009") )
            studentName =  "陈健丁";
        else if( studentId.equals("G20210123456789"))
            studentName =  "心心";

        System.out.println("请求参数为：" + studentId + " 返回值为：" + studentName);
        return studentName;
    }
}
