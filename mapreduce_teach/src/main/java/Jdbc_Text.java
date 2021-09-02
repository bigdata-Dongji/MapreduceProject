import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Jdbc_Text {
    public static void main(String[] args) throws Exception {
//        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
//        String endtime="2020-02-02";
//        String st="1506628174";
//        String format = dateFormat.format(new Date(Long.parseLong(st)*1000));
//        System.out.println(format);
        String driver = "com.mysql.jdbc.Driver";   //数据库驱动
        String url = "jdbc:mysql://localhost/mapreducedb"; //数据库地址
        String username = "root";// 数据库的用户名
        String password = "511722";// 数据库的密码
        Class.forName(driver);//加载数据库驱动
        Connection connection = DriverManager.getConnection(url, username, password);//获取连接对象

        /*
        * 对用户输入数据的合法性没有判断或过滤不严，攻击者可以在web应用程序中事先定义好的查询语句的结尾上添加额外的SQL语句，
        * 在管理员不知情的情况下实现非法操作，以此来实现欺骗数据库服务器执行非授权的任意查询，从而进一步得到相应的数据信息。
        *
        * 简单来说，通过拼装sql语句，让原本的语义改变了，就叫做sql注入
        * */
        //'suibian' or '1'='1',万能密码,可以通过Statement
        String sql="select student_name from student where student_no='asd' and password='suibian' or '1'='1'";//定义要执行的sql命令
        Statement statement = connection.createStatement();//通过连接对象将命令运输到数据库
        //execute增删改等操作。
        statement.execute(sql);
        //execute查询操作。返回结果集合resultSet
        ResultSet resultSet = statement.executeQuery(sql);
        //从结果集对象获取数据
        while (resultSet.next()){//有数据就返回true
            //resultSet的方法getString，获取数据，可以指定列名，列索引
            System.out.println(resultSet.getString(""));
        }
        //关闭资源对象使用的顺序，倒序
        resultSet.close();
        statement.close();
        connection.close();

        String no=null;
        String pwd=null;

        /*
        * 如何避免sql注入的隐患
        * 使用PreparedStatement接口，继承自Statement接口，比Statement对象更灵活，更有效率，提高了安全性
        *
        * 'suibian' or '1'='1',万能密码,通不过Statement
        * */
        String sqls="select student_name from student where student_no=? and password=?";
        PreparedStatement ps = connection.prepareStatement(sqls);
        //给占位符？号，赋值
        ps.setString(1,no);
        ps.setString(2,pwd);

        ResultSet rs = ps.executeQuery();
//        int i = ps.executeUpdate();
    }
}
