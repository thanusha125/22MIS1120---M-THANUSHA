import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class CityDataMapReduce {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
        private Text outputKey = new Text();
        private Text outputValue = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\t");
            if (fields.length == 21 && !fields[0].equals("name_of_city")) {
                String city = fields[0];
                String state = fields[2];
                String totalPopulation = fields[4];
                String malePopulation = fields[5];
                String femalePopulation = fields[6];
                String childPopulation = fields[9];
                String literacyRate = fields[15];
                String maleLiteracyRate = fields[16];
                String femaleLiteracyRate = fields[17];
                
                outputKey.set(state);
                outputValue.set("POPULATION:" + totalPopulation +
                                ",MALE:" + malePopulation +
                                ",FEMALE:" + femalePopulation +
                                ",CHILD:" + childPopulation +
                                ",LITERACY:" + literacyRate +
                                ",MALE_LITERACY:" + maleLiteracyRate +
                                ",FEMALE_LITERACY:" + femaleLiteracyRate);

                context.write(outputKey, outputValue);
            }
        }
    }

    public static class AggregatorReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double totalPopulation = 0;
            double totalMale = 0;
            double totalFemale = 0;
            double totalChild = 0;
            double literacyRateSum = 0;
            double maleLiteracyRateSum = 0;
            double femaleLiteracyRateSum = 0;
            int count = 0;

            for (Text value : values) {
                String[] fields = value.toString().split(",");
                double population = Double.parseDouble(fields[0].split(":")[1]);
                double male = Double.parseDouble(fields[1].split(":")[1]);
                double female = Double.parseDouble(fields[2].split(":")[1]);
                double child = Double.parseDouble(fields[3].split(":")[1]);
                double literacy = Double.parseDouble(fields[4].split(":")[1]);
                double maleLiteracy = Double.parseDouble(fields[5].split(":")[1]);
                double femaleLiteracy = Double.parseDouble(fields[6].split(":")[1]);

                totalPopulation += population;
                totalMale += male;
                totalFemale += female;
                totalChild += child;
                literacyRateSum += literacy;
                maleLiteracyRateSum += maleLiteracy;
                femaleLiteracyRateSum += femaleLiteracy;
                count++;
            }

            double averageLiteracyRate = literacyRateSum / count;
            double averageMaleLiteracyRate = maleLiteracyRateSum / count;
            double averageFemaleLiteracyRate = femaleLiteracyRateSum / count;

            result.set("TOTAL_POPULATION:" + totalPopulation +
                       ",AVERAGE_LITERACY_RATE:" + averageLiteracyRate +
                       ",AVERAGE_MALE_LITERACY_RATE:" + averageMaleLiteracyRate +
                       ",AVERAGE_FEMALE_LITERACY_RATE:" + averageFemaleLiteracyRate);

            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "city data mapreduce");
        job.setJarByClass(CityDataMapReduce.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(AggregatorReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
