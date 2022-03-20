# 第三周HBase作业
## 一.代码逻辑
```java
package week3.hbase.hw;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HBaseHW {

    private static Connection conn = null;
    private static Admin admin = null;

    static {
        try{
            // 建立连接
            Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum", "emr-worker-1,emr-worker-2,emr-header-1,");
            configuration.set("hbase.zookeeper.property.clientPort", "2181");
            conn = ConnectionFactory.createConnection(configuration);
            admin = conn.getAdmin();
        }catch (IOException e){
            System.out.println("init error: " + e.getMessage());
        }
    }

    public static void main(String[] args) throws IOException {

        TableName tableName = TableName.valueOf("pandi:student");
        String rowKey = "pandi";

        // 建表
        createTable(tableName, "info", "score");

        // 插入数据
        Map<String, List<Long>> dataMap = new HashMap<>();
        dataMap.put("Tom", Arrays.asList(20210000000001L, 1L, 75L, 82L));
        dataMap.put("Jerry", Arrays.asList(20210000000002L, 1L, 85L, 67L));
        dataMap.put("Jack", Arrays.asList(20210000000003L, 2L, 80L, 80L));
        dataMap.put("Rose", Arrays.asList(20210000000004L, 2L, 60L, 61L));
        dataMap.put(rowKey, Arrays.asList(20220735020155L, 2L, 68L, 72L));

        dataMap.forEach((k,v)->{
            try{
                putData(tableName, k, "info", "student_id", v.get(0).toString());
                putData(tableName, k, "info", "class", v.get(1).toString());
                putData(tableName, k, "score", "understanding", v.get(2).toString());
                putData(tableName, k, "score", "programming", v.get(3).toString());
            } catch (IOException e){
                System.out.println("putData failed: " + e.getMessage());
            }
        });

        // 查看数据
        getData(tableName, rowKey);

        // 删除数据
//        deleteData(tableName, rowKey);

        // 删除表
//        deleteTable(tableName);
    }

    private static void deleteTable(TableName tableName) throws IOException {
        if (admin.tableExists(tableName)) {
            //删表前需disable表
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("Table Delete Successful");
        } else {
            System.out.println("Table does not exist!");
        }
    }

    private static void deleteData(TableName tableName, String rowKey) throws IOException {
        //根据rowkey删除对应数据
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        conn.getTable(tableName).delete(delete);
        System.out.println("Delete Success");
    }

    private static void getData(TableName tableName, String rowKey) throws IOException {
        Get get = new Get(Bytes.toBytes(rowKey));
        if (!get.isCheckExistenceOnly()) {
            //遍历拿到rowkey下的colName和value
            Result result = conn.getTable(tableName).get(get);
            for (Cell cell : result.rawCells()) {
                String colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                System.out.println("Data get success, colName: " + colName + ", value: " + value);
            }
        }
    }

    private static void putData(TableName tableName, String rowKey, String colFamily, String colKey, String colValue) throws IOException {
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(colKey), Bytes.toBytes(colValue));
        Table table = conn.getTable(tableName);
        table.put(put);
        System.out.println("Data insert success");
        table.close();
    }

    private static void createTable(TableName tableName, String... colFamilies) throws IOException {
        //验证表是否存在，是则删除
        if (admin.tableExists(tableName)) {
            deleteTable(tableName);
        } else {
            TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(tableName);
            for(String cf: colFamilies){
                ColumnFamilyDescriptor cfd = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf)).build();
                tdb.setColumnFamily(cfd);
            }
            admin.createTable(tdb.build());
            System.out.println("Table create successful");
        }
    }
}
```
## 二.输出结果
不包含删除表和数据的输出结果
![image](https://user-images.githubusercontent.com/16860476/159157840-cdc33c17-b3b1-4adf-8614-ab6e031bec09.png)
包含删除表和数据的输出结果
![image](https://user-images.githubusercontent.com/16860476/159157895-222e2821-368d-4372-ab30-7f7c9da33cc3.png)
