package src.data;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

// Первые 2 то что принимает (тут он принимает то что мы отдали из мапера), вторые 2 то что отдает
public class SalesReducer extends Reducer<Text, Sales, Text, Text> {

    // Суммируем выручку и количество для каждой категории
    @Override
    protected void reduce(Text key, Iterable<Sales> values, Context context) throws IOException, InterruptedException {
        double totalRevenue = 0;
        int totalQuantity = 0;

        //По сути тут берем все значения по ключу key и считаем их Revenue и Quantity
        for (Sales val : values) {
            totalRevenue += val.getRevenue().get();
            totalQuantity += val.getQuantity().get();
        }

        context.write(key, new Text(String.format("%.2f\t%d", totalRevenue, totalQuantity)));
    }
}

