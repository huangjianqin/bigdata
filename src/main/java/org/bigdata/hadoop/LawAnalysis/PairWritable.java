package org.bigdata.hadoop.LawAnalysis;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

/**
 * Created by 健勤 on 2017/5/21.
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

    public void setPair(FIRST first, SECOND second){
        this.first = first;
        this.second = second;
    }

    public WritableComparable[] getPair(){
        return new WritableComparable[]{this.first, this.second};
    }

    @Override
    public int compareTo(PairWritable<FIRST, SECOND> o) {
        int cmdFirst = -1;
        return ((cmdFirst = getFirst().compareTo(o.getFirst())) != 0)? cmdFirst : getSecond().compareTo(o.getSecond());
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
        if(obj instanceof PairWritable){
            PairWritable other = (PairWritable)obj;
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

    static {
        //为该Writable注册Comparator
        WritableComparator.define(PairWritable.class, new PairComparator());
    }

    public static class PairComparator extends WritableComparator{
        public PairComparator() {
            super(PairWritable.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            try {
                int firstL1S = WritableUtils.decodeVIntSize(b1[s1]);
                int firstL2S = WritableUtils.decodeVIntSize(b2[s2]);
                int firstL1 = firstL1S + readVInt(b1, s1);
                int firstL2 = firstL2S + readVInt(b2, s2);
                int cmd1 = compareBytes(b1, s1 + firstL1S, firstL1, b2, s2 + firstL2S, firstL2);
                if(cmd1 != 0){
                    return cmd1;
                }

                int secondL1S = WritableUtils.decodeVIntSize(b1[s1 + firstL1]);
                int secondL2S = WritableUtils.decodeVIntSize(b2[s2 + firstL2]);

                return compareBytes(b1, s1 + firstL1 + secondL1S, l1 - firstL1 - secondL1S,
                        b2, s2 + firstL2 + secondL2S, l2 - firstL2 - secondL2S);
            } catch (IOException e) {
                e.printStackTrace();
            }

            return -1;
        }
    }

}
