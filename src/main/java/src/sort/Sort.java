package src.sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Sort implements Writable {
    private final Text category = new Text();
    private final IntWritable quantity = new IntWritable();

    public Sort(String category, int quantity) {
        this.category.set(category);
        this.quantity.set(quantity);
    }

    public Sort() {
    }

    public Text getCategory() {
        return category;
    }

    public IntWritable getQuantity() {
        return quantity;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        category.write(out);
        quantity.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        category.readFields(in);
        quantity.readFields(in);
    }
}