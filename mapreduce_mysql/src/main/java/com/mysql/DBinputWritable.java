package com.mysql;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DBinputWritable implements Writable, DBWritable {
    private int id;
    private String name;

    public DBinputWritable() {
    }

    public DBinputWritable(int id, String name) {
        this.id = id;
        this.name = name;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(id);
        dataOutput.writeUTF(name);
    }

    public void readFields(DataInput dataInput) throws IOException {
        id=dataInput.readInt();
        name=dataInput.readUTF();
    }

    public void write(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setInt(1,id);
        preparedStatement.setString(2,name);
    }

    public void readFields(ResultSet resultSet) throws SQLException {
        id=resultSet.getInt(1);
        name=resultSet.getString(2);
    }

}
