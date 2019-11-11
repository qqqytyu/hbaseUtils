package lll.hadoop.hbase.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @ClassName HbaseUtils
 * @Description TODO Hbase连接工具
 * @Author liujx
 * @Create 2019-03-28 10:47
 * @Version 1.0
 */
public class HbaseUtils {

    //Hadoop config
    private Configuration conf;

    //Hbase 连接
    private Connection conn = null;

//    private Admin admin = null;

    //Hbase scan 客户端每次取多少行
    private int cache = 1000;

    //scan 是否包含起始数据
    private boolean includeStart = true;

    //sacn 是否包含结束数据
    private boolean includeStop = true;

    private Put hput = null;

    private List<Put> hputs = null;

    //hputs最大存储条数
    private int hput_max_size = 1000;

    private static Logger logger = LoggerFactory.getLogger(HbaseUtils.class);

    //初始化连接
    public HbaseUtils(String zkList, String zkPort) throws Exception {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum",zkList);
        conf.set("hbase.zookeeper.property.clientPort",zkPort);
        longConnection();
    }

    //保持连接
    private void longConnection() throws Exception {
        int num = 0;
        while(true){
            try{
                getConnection();
                break;
            }catch (Exception ex){
                if(num++>9)
                    throw ex;
                else logger.warn(String.format("Hbase Connection is interruption, Retry Count: %d",num));
            }
            Thread.sleep(1000);
        }
    }

    //获取连接
    private void getConnection() throws IOException {

        if(conn == null || conn.isClosed())
            conn = ConnectionFactory.createConnection(conf);

//        if(admin == null || admin.isAborted())
//            admin = conn.getAdmin();

    }

    /**
     * 插入一个cell
     * @param tableName 表名
     * @param rowKey rowkey
     * @param family 列簇名
     * @param column 列名
     * @param value 值
     * @return 是否成功
     */
    public boolean insertCell(String tableName, String rowKey, String family, String column, String value){
        Table table = null;
        try {
            longConnection();
            table = conn.getTable(TableName.valueOf(tableName));
            Put put = new Put(rowKey.getBytes());
            put.addColumn(family.getBytes(), column.getBytes(), value.getBytes());
            table.put(put);
            return true;
        }catch (Exception ex){
            logger.error(ex.getMessage(), ex);
            return false;
        }finally {
            closeTable(table);
        }
    }

    /**
     * 同一个列簇一次性插入多个cell
     * @param tableName 表名
     * @param rowKey rowkey
     * @param family 列簇
     * @param cells cell数据 <列名,值>
     * @return 是否成功
     */
    public boolean insertCells(String tableName, String rowKey, String family, Map<String, String> cells){
        Table table = null;
        try {
            longConnection();
            table = conn.getTable(TableName.valueOf(tableName));
            Put put = new Put(rowKey.getBytes());
            for (Map.Entry<String, String> entry : cells.entrySet()){
                put.addColumn(family.getBytes(), entry.getKey().getBytes(), entry.getValue().getBytes());
            }
            table.put(put);
            return true;
        }catch (Exception ex){
            logger.error(ex.getMessage(), ex);
            return false;
        }finally {
            closeTable(table);
        }
    }

    /**
     * 一次性插入多个cell
     * @param tableName 表名
     * @param rowKey rowkey
     * @param cells cell数据<列簇名<列名,值>>
     * @return 是否成功
     */
    public boolean insertCells(String tableName, String rowKey, Map<String,Map<String,String>> cells){
        Table table = null;
        try {
            longConnection();
            table = conn.getTable(TableName.valueOf(tableName));
            Put put = new Put(rowKey.getBytes());
            for (Map.Entry<String, Map<String,String>> entry : cells.entrySet()){
                for (Map.Entry<String, String> ev : entry.getValue().entrySet()){
                    put.addColumn(entry.getKey().getBytes(), ev.getKey().getBytes(), ev.getValue().getBytes());
                }
            }
            table.put(put);
            return true;
        }catch (Exception ex){
            logger.error(ex.getMessage(), ex);
            return false;
        }finally {
            closeTable(table);
        }
    }

    /**
     * 准备批量插入hput
     */
    public void readyHputs(){
        this.hput = null;
        this.hputs=null;
    }

    /**
     * 创建一个新的hput
     */
    public void createHput(String rowkey){
        this.hput = new Put(rowkey.getBytes());
    }

    /**
     * 将待插入数据存储到hput中
     * @param family 列簇
     * @param column 列
     * @param value 值
     */
    public void saveDataToPut(String family, String column, String value) throws Exception {
        if(this.hput != null)
            this.hput.addColumn(family.getBytes(), column.getBytes(), value.getBytes());
        else throw new Exception("hput is null!");
    }

    /**
     * 将hput存储到list中，等待插入
     * @param tableName 表名
     */
    public void addHput(String tableName){
        if(this.hputs == null)
            this.hputs = new ArrayList<>();
        if(this.hputs.size() >= this.hput_max_size){
            saveHputs(tableName);
            this.hputs.clear();
        }
        this.hputs.add(this.hput);
        this.hput = null;
    }

    /**
     * 将hputs提交到hbase
     * @param tableName 表名
     * @return 是否成功
     */
    public boolean saveHputs(String tableName){
        Table table = null;
        try {
            if(this.hputs == null)
                return false;
            longConnection();
            table = conn.getTable(TableName.valueOf(tableName));
            table.put(this.hputs);
            this.hputs = null;
            return true;
        }catch (Exception ex){
            logger.error(ex.getMessage(), ex);
            return false;
        }finally {
            closeTable(table);
        }
    }


    /**
     * 根据rowkey返回一行数据
     * @param tableName 表名
     * @param rowKey rowkey
     * @return 返回一个map数据，<"Family:Qualifier","Value">
     */
    public Map<String,String> getOneRow(String tableName, String rowKey){
        return getOneRow(tableName, rowKey, null);
    }

    /**
     * 根据rowkey返回一行指定列数据
     * @param tableName 表名
     * @param rowKey rowkey
     * @param colls 指定列名 <"Family","Qualifier">
     * @return 返回一个map数据，<"Family:Qualifier","Value">
     */
    public Map<String,String> getOneRow(String tableName, String rowKey, Map<String,String> colls){
        Table table = null;
        Map<String,String> returnData = new HashMap<>();
        try {
            longConnection();
            table = conn.getTable(TableName.valueOf(tableName));
            Get get = new Get(rowKey.getBytes());
            if(colls != null)
                for(Map.Entry<String, String> entry : colls.entrySet())
                    get.addColumn(entry.getKey().getBytes(), entry.getValue().getBytes());
            Result result = table.get(get);
            cellsToMap(result.listCells(), returnData);
            return returnData;
        }catch (Exception ex){
            logger.error(ex.getMessage(), ex);
            return null;
        }finally {
            closeTable(table);
        }
    }

    /**
     * 前序匹配rowkey值，进行scan扫表
     * @param tableName 表名
     * @param rowKeyPrefix rowkey
     * @return 所有符合条件的值 <"Family:Qualifier","Value">
     */
    public Map<String,Map<String,String>> getRowsByPrefix(String tableName, String rowKeyPrefix){
        return getRowsByPrefix(tableName, rowKeyPrefix, null);
    }

    /**
     * 前序匹配rowkey值并指定返回列值，进行scan扫表
     * @param tableName 表名
     * @param rowKeyPrefix rowkey
     * @param colls 指定的列值
     * @return 所有符合条件的值 <"Family:Qualifier","Value">
     */
    public Map<String,Map<String,String>> getRowsByPrefix(String tableName, String rowKeyPrefix,Map<String,String> colls){
        Table table = null;
        Map<String,Map<String,String>> returnData = new HashMap<>();
        try {
            longConnection();
            table = conn.getTable(TableName.valueOf(tableName));
            PrefixFilter filter = new PrefixFilter(rowKeyPrefix.getBytes());
            Scan scan = new Scan();
            if(colls != null){
                for(Map.Entry<String, String> entry : colls.entrySet())
                    scan.addColumn(entry.getKey().getBytes(), entry.getValue().getBytes());
                //设置scan一行取出多少列
                scan.setBatch(colls.size());
            }
            scan.setFilter(filter);
            //设置cache缓存，一次取出多少行
            scan.setCaching(cache);
            ResultScanner scanner = table.getScanner(scan);
            for(Result rs : scanner){
                returnData.put(Bytes.toString(rs.getRow()),cellsToMap(rs.listCells(), new HashMap<>()));
            }
            return returnData;
        }catch (Exception ex){
            logger.error(ex.getMessage(), ex);
            return null;
        }finally {
            closeTable(table);
        }
    }

    /**
     * 通过起止rowkey进行scan扫表
     * @param tableName 表名
     * @param startRow 起始rowkey
     * @param stopRow 终止rowkey
     * @return 所有符合条件的值 <"Family:Qualifier","Value">
     */
    public Map<String,Map<String,String>> getRowsByRange(String tableName,String startRow,String stopRow){
        return getRowsByRange(tableName, startRow, stopRow, null);
    }

    /**
     * 通过起止rowkey进行scan扫表并指定返回列值
     * @param tableName 表名
     * @param startRow 起始rowkey
     * @param stopRow 终止rowkey
     * @param colls 指定的列值
     * @return 所有符合条件的值 <"Family:Qualifier","Value">
     */
    public Map<String,Map<String,String>> getRowsByRange(String tableName,String startRow,String stopRow, Map<String,String> colls){
        Table table = null;
        Map<String,Map<String,String>> returnData = new HashMap<>();
        try {
            longConnection();
            table = conn.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            scan.withStartRow(startRow.getBytes(),includeStart);
            scan.withStopRow(stopRow.getBytes(),includeStop);
            if(colls != null){
                for(Map.Entry<String, String> entry : colls.entrySet())
                    scan.addColumn(entry.getKey().getBytes(), entry.getValue().getBytes());
                scan.setBatch(colls.size());
            }
            scan.setCaching(cache);
            ResultScanner scanner = table.getScanner(scan);
            for(Result rs : scanner){
                returnData.put(Bytes.toString(rs.getRow()),cellsToMap(rs.listCells(), new HashMap<>()));
            }
            return returnData;
        }catch (Exception ex){
            logger.error(ex.getMessage(), ex);
            return null;
        }finally {
            closeTable(table);
        }
    }

    /**
     *将cell转换为map
     */
    private Map<String,String> cellsToMap(List<Cell> cells, Map<String,String> map){
        for(Cell cell : cells){
            map.put(
                    String.format(
                            "%s:%s",
                            Bytes.toString(CellUtil.cloneFamily(cell)),
                            Bytes.toString(CellUtil.cloneQualifier(cell))),
                    Bytes.toString(CellUtil.cloneValue(cell)));
        }
        return map;
    }

    /**
     * 前序匹配rowkey来批量删除数据
     * @param tableName 表名
     * @param rowKeyPrefix rowkey
     * @return 是否成功
     */
    public boolean deleteByPrefix(String tableName, String rowKeyPrefix){
        return deleteByPrefix(tableName, rowKeyPrefix, null, null);
    }

    /**
     * 前序匹配rowkey来批量删除数据，设定一个列值来优化查询速度
     * @param tableName 表名
     * @param rowKeyPrefix rowkey
     * @param family 列簇
     * @param qualifier 列
     * @return 是否成功
     */
    public boolean deleteByPrefix(String tableName, String rowKeyPrefix, String family, String qualifier){

        Table table = null;
        List<Delete> dels = new ArrayList<>();
        try{
            longConnection();
            table = conn.getTable(TableName.valueOf(tableName));
            PrefixFilter filter = new PrefixFilter(rowKeyPrefix.getBytes());
            Scan scan = new Scan();
            scan.setFilter(filter);
            if(family != null && qualifier != null)
                scan.addColumn(family.getBytes(),qualifier.getBytes());
            scan.setCaching(cache);
            ResultScanner scanner = table.getScanner(scan);
            for(Result rs : scanner){
                dels.add(new Delete(rs.getRow()));
            }
            table.delete(dels);
            return true;
        }catch (Exception ex){
            logger.error(ex.getMessage(), ex);
            return false;
        }finally {
            closeTable(table);
        }

    }

    /**
     * 根据起止rowkey来删除数据
     * @param tableName 表名
     * @param startRow 起始rowkey
     * @param stopRow 终止rowkey
     * @return 是否成功
     */
    public boolean deleteByRange(String tableName,String startRow,String stopRow){
        return deleteByRange(tableName, startRow, stopRow, null, null);
    }

    /**
     * 根据起止rowkey来删除数据，设定一个列值来优化查询速度
     * @param tableName 表名
     * @param startRow 起始rowkey
     * @param stopRow 终止rowkey
     * @param family 列簇
     * @param qualifier 列
     * @return 是否成功
     */
    public boolean deleteByRange(String tableName,String startRow,String stopRow, String family, String qualifier){
        Table table = null;
        List<Delete> dels = new ArrayList<>();
        try {
            longConnection();
            table = conn.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            scan.withStartRow(startRow.getBytes(),includeStart);
            scan.withStopRow(stopRow.getBytes(),includeStop);
            if(family != null && qualifier != null)
                scan.addColumn(family.getBytes(),qualifier.getBytes());
            scan.setCaching(cache);
            ResultScanner scanner = table.getScanner(scan);
            for(Result rs : scanner){
                dels.add(new Delete(rs.getRow()));
            }
            table.delete(dels);
            return true;
        }catch (Exception ex){
            logger.error(ex.getMessage(), ex);
            return false;
        }finally {
            closeTable(table);
        }
    }

    /**
     * 删除指定cell数据
     * @param tableName 表名
     * @param rowkey rowkey
     * @param family 列簇
     * @param qualifier 列
     * @return 是否删除成功
     */
    public boolean deleteCell(String tableName, String rowkey,String family, String qualifier){
        Table table = null;
        try {
            longConnection();
            table = conn.getTable(TableName.valueOf(tableName));
            Delete del = new Delete(rowkey.getBytes());
            del.addColumn(family.getBytes(), qualifier.getBytes());
            table.delete(del);
            return true;
        }catch (Exception ex){
            logger.error(ex.getMessage(), ex);
            return false;
        }finally {
            closeTable(table);
        }
    }

    /**
     * 删除多个cell
     * @param tableName 表名
     * @param rowkey rowkey
     * @param family 列簇
     * @param colls 多个列值
     * @return 是否删除成功
     */
    public boolean deleteCells(String tableName, String rowkey,String family,List<String> colls){
        Table table = null;
        try {
            longConnection();
            table = conn.getTable(TableName.valueOf(tableName));
            Delete del = new Delete(rowkey.getBytes());
            for(String coll : colls)
                del.addColumn(family.getBytes(), coll.getBytes());
            table.delete(del);
            return true;
        }catch (Exception ex){
            logger.error(ex.getMessage(), ex);
            return false;
        }finally {
            closeTable(table);
        }
    }

    /**
     * 删除多个cell
     * @param tableName 表名
     * @param rowkey rowkey
     * @param map <"列簇","列">
     * @return 是否删除成功
     */
    public boolean deleteCells(String tableName, String rowkey, Map<String, List<String>> map){
        Table table = null;
        try {
            longConnection();
            table = conn.getTable(TableName.valueOf(tableName));
            Delete del = new Delete(rowkey.getBytes());
            for (Map.Entry<String, List<String>> entry : map.entrySet())
                for(String val : entry.getValue())
                    del.addColumn(entry.getKey().getBytes(), val.getBytes());
            table.delete(del);
            return true;
        }catch (Exception ex){
            logger.error(ex.getMessage(), ex);
            return false;
        }finally {
            closeTable(table);
        }
    }

    private void closeTable(Table table){
        try {
            if(table != null) table.close();
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    //设置scan一次取出的行数
    public void setCache(int cache) {
        this.cache = cache;
    }

    //设置scan扫表是否包含起始行
    public void setIncludeStart(boolean includeStart) {
        this.includeStart = includeStart;
    }

    //设置scan扫表是否包含终止行
    public void setIncludeStop(boolean includeStop) {
        this.includeStop = includeStop;
    }

    //设置hputs一次性提交的条数
    public void setHput_max_size(int hput_max_size) {
        this.hput_max_size = hput_max_size;
    }

    public static void main(String[] args) throws Exception {

        HbaseUtils hbaseUtils = new HbaseUtils("kn52.cu-air.com,kn55.cu-air.com,kn56.cu-air.com","2181");

//        hbaseUtils.deleteByRange("cua_caci:flown_seg_sumofday","20190920_000000","20191020_ZZZZZZ", "information", "prep_fare");

    }

}
