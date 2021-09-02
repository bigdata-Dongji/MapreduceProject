package recruit;


import com.alibaba.fastjson.JSON;

//import org.apache.hadoop.hbase.client.Put;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
/*
* 数据如下data.json：
* id	company_name	eduLevel_name	emplType	jobName	salary	createDate	endDate	city_code	companySize	welfare	responsibility	place	workingExp
* id编号	公司名称	学历要求	工作类型	工作名称	薪资	发布时间	截止时间	城市编码	公司规模	福利	岗位职责	地区	工作经验
*
* 城市编码表province：
* city_code	varchar(255)		城市编码
* city_name	varchar(255)		城市名称
*
*
* 清洗规则：
* 若某个属性为空则删除这条数据；
* 处理数据中的salary： 1)mK-nK：(m+n)/2； 2)其余即为0。
* 按照MySQL表province 将城市编码转化为城市名；
* 将结果存入MySQL表job中；
* */
public class JsonMap extends Mapper<LongWritable,Text,Jsonrecruit,NullWritable>{
    private Map<String,String> map = new HashMap<String, String>();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Connection conn = DBHelper.getConn();
        String sql="SELECT * FROM province";
        try {
            PreparedStatement ps = conn.prepareStatement(sql);
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()){
                String index = resultSet.getString(1);
                String city = resultSet.getString(2);
                map.put(index,city);
            }
            resultSet.close();
            ps.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        DBHelper.closeConnection();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        System.out.println("清洗json数据中");
        JSONObject jsonObject = JSON.parseObject(value.toString());
        String id = jsonObject.getString("id");
        String company_name = jsonObject.getString("company_name");
        String eduLevel_name = jsonObject.getString("eduLevel_name");
        String emplType = jsonObject.getString("emplType");
        String jobName = jsonObject.getString("jobName");
        String salary = jsonObject.getString("salary");
        String createDate = jsonObject.getString("createDate");
        String endDate = jsonObject.getString("endDate");
        String city_code = jsonObject.getString("city_code");
        String companySize = jsonObject.getString("companySize");
        String welfare = jsonObject.getString("welfare");
        String responsibility = jsonObject.getString("responsibility");
        String place = jsonObject.getString("place");
        String workingExp = jsonObject.getString("workingExp");
        //若某个属性为空则删除这条数据；
        if (id.isEmpty()||company_name.isEmpty()||eduLevel_name.isEmpty()||emplType.isEmpty()||jobName.isEmpty()||salary.isEmpty()||createDate.isEmpty()||endDate.isEmpty()||city_code.isEmpty()||companySize.isEmpty()||welfare.isEmpty()||responsibility.isEmpty()||place.isEmpty()||workingExp.isEmpty()){
            return;
        }
        //处理数据中的salary： 1)mK-nK：(m+n)/2； 2)其余即为0。
        String[] split = salary.split("-");
        int avg=(Integer.parseInt(split[0].replace("K",""))+Integer.parseInt(split[1].replace("K","")))/2;
        salary=avg+"K";
        //按照MySQL表province 将城市编码转化为城市名；
        String city=map.get(city_code);
        Jsonrecruit jsonrecruit = new Jsonrecruit(id,company_name,eduLevel_name,emplType,jobName,salary,createDate,endDate,city,companySize,welfare,responsibility,place,workingExp);
        context.write(jsonrecruit,NullWritable.get());
    }
}

