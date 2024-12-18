package src.sort;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortReducer extends Reducer<DoubleWritable, Sort, Text, Text> {

    @Override
    protected void reduce(DoubleWritable key, Iterable<Sort> values, Context context) throws IOException, InterruptedException {
        for (Sort value : values) {
            context.write(
                    //Тут обратно вернули что бы в выводе было category
                    new Text(value.getCategory()),
                    //И снова развернули ревеню так как до этого его разворачивали для сортировки
                    new Text(String.format("%.2f\t%d", -1 * key.get(), value.getQuantity().get()))
            );
        }
    }
}