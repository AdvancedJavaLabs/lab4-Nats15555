package src.data;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

// Первые 2 то что принимает, вторые 2 то что отдает
public class SalesMapper extends Mapper<Object, Text, Text, Sales> {

    //Мапит все строчки из csv файлов
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        if (fields.length < 5 || value.toString().startsWith("transaction_id")) {
            return;
        }

        context.write(
                //category
                new Text(fields[2]),
                //revenue(выручка цена * кол-во) , quantity
                new Sales(Double.parseDouble(fields[3]) * Integer.parseInt(fields[4]), Integer.parseInt(fields[4]))
        );
    }
}
