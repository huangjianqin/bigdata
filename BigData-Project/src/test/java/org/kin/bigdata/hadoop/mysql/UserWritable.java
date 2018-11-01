package org.kin.bigdata.hadoop.mysql;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by 健勤 on 2017/7/19.
 */
public class UserWritable implements DBWritable, Writable {
    private int id;
    private String name;
    private String description;

    public UserWritable() {
    }

    public UserWritable(int id) {
        this.id = id;
    }

    public UserWritable(int id, String name) {
        this.id = id;
        this.name = name;
    }

    public UserWritable(int id, String name, String description) {
        this.id = id;
        this.name = name;
        this.description = description;
    }

    @Override
    public void write(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setInt(1, id);
        preparedStatement.setString(2, name);
        preparedStatement.setString(3, description);
//        preparedStatement.setString(1, name);
//        preparedStatement.setString(2, description);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        id = resultSet.getInt(1);
        name = resultSet.getString(2);
        description = resultSet.getString(3);
//        name = resultSet.getString(1);
//        description = resultSet.getString(2);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(id);
        Text.writeString(dataOutput, name);
        Text.writeString(dataOutput, description);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        id = dataInput.readInt();
        name = Text.readString(dataInput);
        description = Text.readString(dataInput);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof UserWritable)) return false;

        UserWritable that = (UserWritable) o;

        return id == that.id;
    }


    @Override
    public int hashCode() {
        return id;
    }

    @Override
    public String toString() {
        return id + "," + name + "," + description;
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

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

}
