package src.sort;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<Object, Text, DoubleWritable, Sort> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        if (fields.length < 3) {
            return;
        }

        context.write(
                //-1 * Double.parseDouble(fields[1]) делает сортировку по убыванию (revenue)
                new DoubleWritable(-1 * Double.parseDouble(fields[1])),
                //category, quantity
                new Sort(fields[0], Integer.parseInt(fields[2]))
        );
    }
}