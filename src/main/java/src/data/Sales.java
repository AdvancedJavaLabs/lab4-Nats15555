package src.data;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Sales implements Writable {

    private final DoubleWritable revenue = new DoubleWritable();
    private final IntWritable quantity = new IntWritable();

    public Sales(double revenue, int quantity) {
        this.revenue.set(revenue);
        this.quantity.set(quantity);
    }

    public Sales() {
    }

    public DoubleWritable getRevenue() {
        return revenue;
    }

    public IntWritable getQuantity() {
        return quantity;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        revenue.write(out);
        quantity.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        revenue.readFields(in);
        quantity.readFields(in);
    }
}

