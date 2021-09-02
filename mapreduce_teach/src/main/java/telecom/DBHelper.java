package telecom;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**连接MySQL工具类*/
public class DBHelper {
    private static final String driver = "com.mysql.jdbc.Driver";   //数据库驱动
    private static final String url = "jdbc:mysql://localhost/mapreducedb"; //数据库地址
    private static final String username = "root";// 数据库的用户名
    private static final String password = "511722";// 数据库的密码

    private static Connection conn;

    // 获取连接对象
    public static Connection getConn(){


        try {
            Class.forName(driver);  //加载数据库驱动
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }


        try {
            conn = DriverManager.getConnection(url,username,password);  //建立连接对象
        } catch (SQLException e) {
            e.printStackTrace();
        }


        return conn;    //返回连接对象
    }

    public static void closeConnection(){
        if(conn != null){
            try {
                conn.close();   //关闭连接
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

}