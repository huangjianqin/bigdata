package org.kin.bigdata.hadoop.common.writable;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by 健勤 on 2017/5/21.
 * 两个值不能为Null
 * 本质上只是基类而已
 */
public class PairWritable<FIRST extends WritableComparable, SECOND extends WritableComparable> implements WritableComparable<PairWritable<FIRST, SECOND>> {
    private FIRST first;
    private SECOND second;

    public PairWritable(FIRST first, SECOND second) {
        setPair(first, second);
    }

    public void setFirst(FIRST first) {
        this.first = first;
    }

    public void setSecond(SECOND second) {
        this.second = second;
    }

    public FIRST getFirst() {
        return this.first;
    }

    public SECOND getSecond() {
        return this.second;
    }

    public void setPair(FIRST first, SECOND second) {
        if (first == null || second == null) {
            throw new IllegalArgumentException("unsupport args is null");
        }

        this.first = first;
        this.second = second;
    }

    public WritableComparable[] getPair() {
        return new WritableComparable[]{this.first, this.second};
    }

    @Override
    public int compareTo(PairWritable<FIRST, SECOND> o) {
        int cmdFirst = -1;
        return ((cmdFirst = getFirst().compareTo(o.getFirst())) != 0) ? cmdFirst : getSecond().compareTo(o.getSecond());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.first.write(dataOutput);
        this.second.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.first.readFields(dataInput);
        this.second.readFields(dataInput);
    }

    @Override
    public int hashCode() {
        return this.first.hashCode() * 31 + this.second.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof PairWritable) {
            PairWritable other = (PairWritable) obj;
            WritableComparable oFirst = other.getFirst();
            WritableComparable oSecond = other.getSecond();

            return getFirst().equals(oFirst) && getSecond().equals(oSecond);
        }

        return false;
    }

    @Override
    public String toString() {
        return "(" + getFirst().toString() + ", " + getSecond().toString() + ")";
    }

}
