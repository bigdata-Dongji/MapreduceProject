package recruit;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Jsonrecruit implements WritableComparable, DBWritable {
    private String id ;
    private String company_name ;
    private String eduLevel_name ;
    private String emplType ;
    private String jobName ;
    private String salary ;
    private String createDate ;
    private String endDate ;
    private String city_name ;
    private String companySize ;
    private String welfare ;
    private String responsibility ;
    private String place ;
    private String workingExp ;

    public Jsonrecruit() {
    }

    public Jsonrecruit(String id, String company_name, String eduLevel_name, String emplType, String jobName, String salary, String createDate, String endDate, String city_name, String companySize, String welfare, String responsibility, String place, String workingExp) {
        this.id = id;
        this.company_name = company_name;
        this.eduLevel_name = eduLevel_name;
        this.emplType = emplType;
        this.jobName = jobName;
        this.salary = salary;
        this.createDate = createDate;
        this.endDate = endDate;
        this.city_name = city_name;
        this.companySize = companySize;
        this.welfare = welfare;
        this.responsibility = responsibility;
        this.place = place;
        this.workingExp = workingExp;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(id);
        dataOutput.writeUTF(company_name);
        dataOutput.writeUTF(eduLevel_name);
        dataOutput.writeUTF(emplType);
        dataOutput.writeUTF(jobName);
        dataOutput.writeUTF(salary);
        dataOutput.writeUTF(createDate);
        dataOutput.writeUTF(endDate);
        dataOutput.writeUTF(city_name);
        dataOutput.writeUTF(companySize);
        dataOutput.writeUTF(welfare);
        dataOutput.writeUTF(responsibility);
        dataOutput.writeUTF(place);
        dataOutput.writeUTF(workingExp);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        id = dataInput.readUTF();
        company_name = dataInput.readUTF();
        eduLevel_name = dataInput.readUTF();
        emplType = dataInput.readUTF();
        jobName = dataInput.readUTF();
        salary = dataInput.readUTF();
        createDate = dataInput.readUTF();
        endDate = dataInput.readUTF();
        city_name = dataInput.readUTF();
        companySize = dataInput.readUTF();
        welfare = dataInput.readUTF();
        responsibility = dataInput.readUTF();
        place = dataInput.readUTF();
        workingExp = dataInput.readUTF();
    }

    @Override
    public void write(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setString(1,this.id);
        preparedStatement.setString(2,this.company_name);
        preparedStatement.setString(3,this.eduLevel_name);
        preparedStatement.setString(4,this.emplType);
        preparedStatement.setString(5,this.jobName);
        preparedStatement.setString(6,this.salary);
        preparedStatement.setString(7,this.createDate);
        preparedStatement.setString(8,this.endDate);
        preparedStatement.setString(9,this.city_name);
        preparedStatement.setString(10,this.companySize);
        preparedStatement.setString(11,this.welfare);
        preparedStatement.setString(12,this.responsibility);
        preparedStatement.setString(13,this.place);
        preparedStatement.setString(14,this.workingExp);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        this.id=resultSet.getString(1);
        this.company_name=resultSet.getString(2);
        this.eduLevel_name=resultSet.getString(3);
        this.emplType=resultSet.getString(4);
        this.jobName=resultSet.getString(5);
        this.salary=resultSet.getString(6);
        this.createDate=resultSet.getString(7);
        this.endDate=resultSet.getString(8);
        this.city_name=resultSet.getString(9);
        this.companySize=resultSet.getString(10);
        this.welfare=resultSet.getString(11);
        this.responsibility=resultSet.getString(12);
        this.place=resultSet.getString(13);
        this.workingExp=resultSet.getString(14);
    }

    @Override
    public int compareTo(Object o) {
        return 0;
    }
}
