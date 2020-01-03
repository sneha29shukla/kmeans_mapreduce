package writables;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class Point implements Writable {
    private Text dataLabel;
    private ArrayPrimitiveWritable vector = null;
    private IntWritable number = null;

    public Point() {
        dataLabel = new Text();
        vector = new ArrayPrimitiveWritable();
        number = new IntWritable(0);
    }

    public Point(double[] vector, int number, String dataLabel) {
        this();
        setVector(vector);
        setNumber(number);
        this.dataLabel.set(dataLabel);
    }

    public Point(Point point) {
        this();
        this.dataLabel.set(point.getDataLabel());
        double[] vector = point.getVector();
        setVector(Arrays.copyOf(vector, vector.length));
        setNumber((int) point.getNumber());
    }

    public double[] getVector() {
        return (double[]) vector.get();
    }

    public Text getDataLabel() {
        return dataLabel;
    }

    public double getNumber() {
        return (double) number.get();
    }

    public void setVector(double[] vector) {
        this.vector.set(vector);
    }

    public void setNumber(int number) {
        this.number.set(number);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataLabel.write(dataOutput);
        vector.write(dataOutput);
        number.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        dataLabel.readFields(dataInput);
        vector.readFields(dataInput);
        number.readFields(dataInput);
    }

    @Override
    public String toString() {
        double[] thisVector = this.getVector();
        StringBuilder sb = new StringBuilder();
        sb.append(this.dataLabel).append(" ");
        for (int i = 0, j = thisVector.length; i < j; i++) {
            sb.append(thisVector[i]);
            if (i < thisVector.length - 1) {
                sb.append(" ");
            }
        }
        return sb.toString();
    }

    public void parse(String values) {
        String[] coords = values.split(" ");
        double[] tmp = new double[coords.length - 1];
        for (int i = 0; i < tmp.length; i++) {
            tmp[i] = Double.valueOf(coords[i + 1]);
        }
        dataLabel.set(coords[0]);
        vector.set(tmp);
        number.set(1);
    }

    public void add(Point point) {
        double[] thisVector = this.getVector();
        double[] pointVector = point.getVector();
        for (int i = 0; i < thisVector.length; i++) {
            thisVector[i] += pointVector[i];
        }
        number.set(number.get() + (int) point.getNumber());
    }

    public void compress() {
        double[] thisVector = this.getVector();
        double currentNum = number.get();
        for (int i = 0; i < thisVector.length; i++) {
            thisVector[i] /= currentNum;
        }

        number.set(1);
        this.vector.set(thisVector);
    }

//    @Override
//    public int compareTo(Point o) {
//        return this.dataLabel.compareTo(o.dataLabel);
//    }
}
