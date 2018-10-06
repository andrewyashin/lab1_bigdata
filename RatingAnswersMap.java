import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import com.opencsv.CSVParser;


import java.io.IOException;

public class RatingAnswersMap extends Mapper<Object, Text, IntWritable, Text> {
    private IntWritable keyOut = new IntWritable();
    private static final int SCORE_INDEX = 4;

    private static final Log LOG = LogFactory.getLog(RatingAnswersMap.class);

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        CSVParser parser = new CSVParser();
        String[] lines = parser.parseLineMulti(value.toString());
        try {
	        if (lines.length == 0 
	        		|| !lines[0].matches("[0-9]+") 
	        		|| !lines[SCORE_INDEX+1].matches("TRUE|FALSE")) {
	            return;
	        }
	
	        keyOut.set(Integer.parseInt(lines[SCORE_INDEX]));
	        context.write(keyOut, value);
        } catch (Exception e) {
        }
    }
}
