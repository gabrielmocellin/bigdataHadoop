package questoes.q5;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class transactionBrazilYearMapper extends Mapper<LongWritable, Text, Text, averageWritable> {
    // Função utilizada para o Mapeamento dos dados.
    public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
        String line = value.toString();
        String columnHeadName = "country_or_area";
        Boolean isColumnHead = line.startsWith(columnHeadName);
        String[] columnsArray = line.split(";"); // Transformar/dividr os valores da linha atual em Array.

        String countryValue = columnsArray[0];
        Boolean isNotBrazilData = !countryValue.equals("Brazil");

        if (isColumnHead || isNotBrazilData) return;

        String yearValue = columnsArray[1];
        Text outputKey = new Text(yearValue);

        Long tradeValue = Long.parseLong(columnsArray[5]);
        Long amount = 1L;

        averageWritable avgWritable = new averageWritable(tradeValue, amount);

        con.write(outputKey, avgWritable);
    }
}
