package com.dd.hbase;

import org.apache.hadoop.hbase.client.*;

import org.apache.hadoop.hbase.TableName;

import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * HBase操作工具类
 * 实现Hbase的增删改查操作
 *
 */
public class HBaseUtil {
    private final static Logger logger =  LoggerFactory.getLogger(HBaseUtil.class);

    /**
     * 判断表是否存在
     * @param conn
     * @param tableName
     * @return
     */
    public static boolean existTable(Connection conn, String tableName ) {
        try {
            Admin admin = conn.getAdmin();
            if( admin.tableExists(TableName.valueOf( tableName ) )) {
                logger.info(" {}表存在！", tableName);
                return true;
            }
        }catch (Exception ex) {
            logger.error("判断{}表是否存在过现异常，异常信息为：{}" , tableName,ex.getMessage());
        }
        return false;
    }


    /**
     * 创建表
     * @param tableName 创建表的表名称
     * @param cfs 列簇的集合
     * @return
     */
    public static boolean createTable(Connection conn, String tableName, String[] cfs) {
        if( existTable(conn,tableName )) {
            logger.info("{} 表已存在!, 不能创建新表",tableName);
            return false;
        }

        try {
            Admin admin = conn.getAdmin();
            TableName table= TableName.valueOf (tableName );
            //表描述器构造器
            TableDescriptorBuilder tdb =TableDescriptorBuilder.newBuilder(table);

            List<ColumnFamilyDescriptor> list = new ArrayList<ColumnFamilyDescriptor>();
            for (String familyName : cfs ) {
                //列族描述起构造器
                ColumnFamilyDescriptorBuilder cdb = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(familyName));
                //获得列族描述起
                ColumnFamilyDescriptor cfd = cdb.build();
                list.add(cfd);
            };

            //添加列族
            tdb.setColumnFamilies(list);
            //获得表描述器
            TableDescriptor td = tdb.build();
            //创建表
            admin.createTable(td);
            logger.info("{} 表创建成功!",tableName);

        }
        catch (Exception ex ) {
            logger.info("{} 表创建失败!",tableName);
            return false;
        }

        return true;
    }

    /**
     * 删除表
     * @param tableName 表名称
     * @return
     */
    public static boolean deleteTable(Connection conn, String tableName){

        try{
            if( !existTable(conn, tableName ) ) {
                logger.info("{} 表不存在！", tableName);
                return false;
            }

            Admin admin = conn.getAdmin();
            admin.disableTable(TableName.valueOf( tableName ));
            admin.deleteTable(TableName.valueOf( tableName ));
            logger.info("删除表 {} 成功！", tableName);
        }catch (Exception ex){
            logger.warn("删除表 {} 失败！，异常信息为：{}", tableName, ex.getMessage());
            return false;
        }
        return true;
    }

    /**
     * 插入数据
     * @param tableName 表名
     * @param rowkey
     * @param cfName 列簇名
     * @param qualifer
     * @param data
     * @return
     */
    public static boolean putRow(Connection conn, String tableName,String rowkey,String cfName,String qualifer,String data){
        try {
            Table table = conn.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowkey));
            put.addColumn(Bytes.toBytes(cfName),Bytes.toBytes(qualifer),Bytes.toBytes(data));
            table.put(put);
            logger.info("插入数据[{} {} {} {} ]到表{}中成功！", rowkey, cfName,qualifer, data, tableName);
        }catch (Exception ex){
            logger.info("插入数据[{} {} {} {} ]到表{}中失败！", rowkey, cfName,qualifer, data, tableName);
            logger.error("异常信息为 {}", ex.getMessage());
            return false;
        }
        return true;
    }

    /**
     * 批量导入数据
     * @param conn
     * @param tableName 表名
     * @param puts
     * @return
     */
    public static boolean putRows(Connection conn, String tableName, List<Put> puts){
        try{
            Table table = conn.getTable(TableName.valueOf(tableName));
            table.put(puts);
            logger.info("批量插入数据到表{}中成功！", tableName);
        }catch (Exception ex){
            logger.error("批量插入数据到表{}中失败！异常信息为: {}", tableName, ex.getMessage());
            return false;
        }
        return true;
    }

    /**
     * 查询单条数据
     * @param tableName 表名
     * @param rowkey
     * @return
     */
    public static Result getRow(Connection conn, String tableName,String rowkey){
        try {
            Table table = conn.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowkey));
            Result result = table.get(get);
            logger.info("查询数据表{}的rowkey={}数据成功！", tableName, rowkey);
            return result;
        }catch (Exception ex){
            logger.error("查询数据表{}的 rowkey={}数据失败！异常信息为：{}", tableName, rowkey, ex.getMessage());
        }
        return null;
    }

    /**
     * 删除行数据
     * @param conn
     * @param tableName 表名
     * @param rowkey
     * @return
     */
    public static boolean deleteRow(Connection conn, String tableName,String rowkey){
        try {
            Table table = conn.getTable(TableName.valueOf(tableName));
            Delete delete = new Delete(Bytes.toBytes(rowkey));
            table.delete(delete);
            logger.info("删除数据表{}的rowkey={}数据成功！", tableName, rowkey);
        }catch (Exception ex){
            logger.error("查询数据表{}的 rowkey={}数据失败！异常信息为：{}", tableName, rowkey, ex.getMessage());
        }
        return true;
    }

    /**
     *删除列簇
     * @param tableName 表名
     * @param cfName 列簇名
     * @return
     */
    public static boolean deleteColumnFamily(Connection conn, String tableName,String cfName){
        try{
            Admin admin = conn.getAdmin();
            admin.deleteColumnFamily(TableName.valueOf(tableName), Bytes.toBytes(cfName));
            logger.info("删除数据表{} 的列簇{}数据成功！", tableName, cfName);
        }catch (Exception ex){
            logger.error("删除数据表{} 的列簇{}数据成功！异常信息为：", tableName, cfName, ex.getMessage());
        }
        return true;
    }


    /**
     * 删除列值(
     * @param conn
     * @param tableName
     * @param rowkey
     * @param cfName
     * @param cqName
     * @return
     */
    public static boolean deleteColumnQualifier(Connection conn, String tableName,String rowkey,String cfName,String cqName){
        try {
            Table table = conn.getTable(TableName.valueOf(tableName));
            Delete delete = new Delete(Bytes.toBytes(rowkey));
            delete.addColumn(Bytes.toBytes(cfName),Bytes.toBytes(cqName));
            table.delete(delete);
            logger.info("删除数据表{} 列簇{}的列{}数据成功！", tableName, cfName, cqName);
        }catch (Exception ex){
            logger.error("删除数据表{} 列簇{}的列{}数据失败！异常信息为：{}", tableName, cfName, cqName, ex.getMessage());
            return false;
        }
        return true;
    }


    public static void main(String []args) {
        Connection conn = HBaseConnectionFactory.getConnection();
        String tableName = "chenjd:student1";
        boolean result =  existTable(conn, tableName);
        System.out.println(tableName + " 表是否存在：" + result );
        String[] cfs = {"info","score"};
        createTable(conn, tableName, cfs);
        HBaseConnectionFactory.closeConn();
    }


}
