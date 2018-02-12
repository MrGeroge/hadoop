package HiveDemo;

import org.junit.Before;
import org.junit.Test;

import java.sql.*;

/**
 * Created by George on 2017/5/17.
 */
public class HiveMain {
    private static Connection connection;
    private static Statement statement;
    @Before
    public void init(){
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        try {
            connection=DriverManager.getConnection("jdbc:hive2://localhost:10000/default","root","");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            statement=connection.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    @Test
    public  void test() throws SQLException {//查询数据Select
        ResultSet resultSet=statement.executeQuery("select * from test");
        while(resultSet.next()){
            System.out.println(resultSet.getInt(1));
            System.out.println(resultSet.getString(2));
        }
    }
    @Test
    public void test1() throws SQLException {//insert
        int row=statement.executeUpdate("insert into test (id,age) values('1','18')");
        System.out.println("row is "+row);
    }
}
