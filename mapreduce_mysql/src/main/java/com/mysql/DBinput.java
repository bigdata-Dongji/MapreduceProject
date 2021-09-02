package com.mysql;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DBinput implements Writable, DBWritable {
    private String word;
    private int num;

    @Override
    public String toString() {
        return word + '\t' + num ;
    }

    public DBinput(String word, int num) {
        this.word = word;
        this.num = num;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(word);
        dataOutput.writeInt(num);
    }

    public void readFields(DataInput dataInput) throws IOException {
        word=dataInput.readUTF();
        num=dataInput.readInt();
    }

    public void write(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setString(1,word);
        preparedStatement.setInt(2,num);

    }

    public void readFields(ResultSet resultSet) throws SQLException {
        word=resultSet.getString(1);
        num=resultSet.getInt(2);
    }
}
