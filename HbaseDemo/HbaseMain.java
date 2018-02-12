package HbaseDemo;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.procedure2.util.StringUtils;
import org.junit.Test;

/**
 * Created by George on 2017/5/18.
 */
public class HbaseMain {
    @Test
    public void test1() throws Exception {//创建表test包含列簇cfi
        Configuration conf = HBaseConfiguration.create();
        HBaseAdmin admin = new HBaseAdmin(conf);
        String table = "test";

        if (admin.isTableAvailable(table)) {
            admin.disableTable(table);
            admin.deleteTable(table);
        } else {
            HTableDescriptor t = new HTableDescriptor(table.getBytes());
            HColumnDescriptor column = new HColumnDescriptor("cfi".getBytes());
            column.setBlockCacheEnabled(true);//启用块缓存
            column.setInMemory(true);//设置读缓存
            column.setMaxVersions(1);//保证列簇中的数据永远是最新的
            t.addFamily(column);
            admin.createTable(t);
        }
    }

    @Test
    public void test2() throws Exception {//增加数据
        Configuration conf = HBaseConfiguration.create();
        HTable hTable = new HTable(conf, "test");
        hTable.setAutoFlush(false);//关闭自动刷新，即put操作只有当缓冲区满时才提交到HBase服务器
        String rowkey = "8283888483818";
        Put put = new Put(rowkey.getBytes());
        put.add("cfi".getBytes(), "name".getBytes(), "caiqi".getBytes());
        put.add("cfi".getBytes(), "sex".getBytes(), "man".getBytes());
        hTable.put(put);
        hTable.close();
    }

    @Test
    public void test3() throws Exception {//查讯
        Configuration conf = HBaseConfiguration.create();
        HTable hTable = new HTable(conf, "test");
        String rowkey = "8283888483818";
        Get get = new Get(rowkey.getBytes());
        Result result = hTable.get(get);
        Cell c = result.getColumnLatestCell("cfi".getBytes(), "name".getBytes());
        System.out.println(new String(new String(CellUtil.cloneValue(c))));
        hTable.close();
    }

    @Test
    public void test4() throws Exception {//scan范围查询
        Configuration conf = HBaseConfiguration.create();
        HTable hTable = new HTable(conf, "test");
        Scan scan = new Scan();
        scan.setStartRow("8283888483818".getBytes());
        scan.setStopRow("8283888483818".getBytes());
        ResultScanner results = hTable.getScanner(scan);
        for (Result result : results) {
            Cell c = result.getColumnLatestCell("cfi".getBytes(), "name".getBytes());
            System.out.println(new String(CellUtil.cloneValue(c)));
        }
        hTable.close();
    }

    @Test
    public void test5() throws Exception {//scan条件查询
        Configuration conf = HBaseConfiguration.create();
        HTable hTable = new HTable(conf, "test");
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);//必须满足全部条件
        SingleColumnValueFilter filter = new SingleColumnValueFilter("cfi".getBytes(), "name".getBytes(), CompareFilter.CompareOp.EQUAL, "caiqi".getBytes());//列值过滤器
        PrefixFilter prefixFilter = new PrefixFilter("8283888".getBytes());
        filterList.addFilter(filter);
        filterList.addFilter(prefixFilter);
        Scan scan = new Scan();
        scan.setFilter(filterList);
        ResultScanner results = hTable.getScanner(scan);
        for (Result result : results) {
            Cell c = result.getColumnLatestCell("cfi".getBytes(), "name".getBytes());
            System.out.println(new String(CellUtil.cloneValue(c)));
        }
        hTable.close();
    }
    @Test
    public void test6() throws InvalidProtocolBufferException {
        HbaseDemo.Test.Order.Builder builder=HbaseDemo.Test.Order.newBuilder();
        builder.setTime(1);
        builder.setDesc("hello world");
        builder.setPrice((float) 0.21);
        builder.setUserid(1);
        System.out.println(builder.build().toString());//结构数据序列化
        HbaseDemo.Test.Order order=HbaseDemo.Test.Order.parseFrom(builder.build().toByteArray());//反序列化
        System.out.println(order.getDesc());
    }
}
