package sa;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

public class SimulatedAnnealingTSP implements Serializable {
    private static final int NUM_NEIGHBORS = 100;
    private static final double INITIAL_TEMPERATURE = 1000;
    private static final double COOLING_RATE = 0.003;
    private static List<int[]> cities;

    /**
     * @return list of cities with a pair of index
     */
    private static List<int[]> readCities(File file) {
        // Create a list of cities and read cities from file
        List<int[]> cities = new ArrayList<>();
        try (Scanner scanner = new Scanner(new FileInputStream(file))) {
            while (scanner.hasNext()) {
                String line = scanner.next();
                if (!"".equals(line.trim())) {
                    String[] lineArgs = line.split("=");
                    String[] positions = lineArgs[1].split(",");
                    cities.add(new int[]{Integer.parseInt(positions[0]), Integer.parseInt(positions[1])});
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(">>> Read Cities size=" + cities.size() + " on=" + file.getAbsolutePath());
        return cities;
    }

    public static void main(String[] args) throws Exception {
	long startTime = System.currentTimeMillis();
        Path input = new Path(args[0]);
        String outputPath = args[1];
        int count = 0;
        while (new File(outputPath).exists()) {
            outputPath = args[1] + "-" + count++;
        }
        Path output = new Path(outputPath);
        System.out.println("input=" + input.getName());
        System.out.println("output=" + output.getName());
        File file = new File(args[2]);
        cities = readCities(file);
        Configuration conf = new Configuration();
        Job job = new Job(conf, "SimulatedAnnealingTSP");
        job.setJarByClass(SimulatedAnnealingTSP.class);
        job.setMapperClass(TSPMapper.class);
        job.setReducerClass(TSPReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, input);
//        FileInputFormat.setInputPaths(job, input);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, output);
//        FileOutputFormat.setOutputPath(job, output);
	long endtime = System.currentTimeMillis();
	long elapsedTime = endTime - startTime;
	double elapsedseconds =elapsedTime / 1000.0;
	System.out.println("Elapsed time: " + elapsedseconds);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class TSPMapper extends Mapper<Object, Text, Text, Text> {
        private final Text keyOut = new Text("0");
        private final Text valueOut = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            long seed = Long.parseLong(value.toString());
            Random random = new Random(seed);
            System.out.println("Random seed=" + seed);
            // Generate a random initial tour
            int[] currentTour = generateInitialTour(random);
            // Perform Simulated Annealing
            double currentDistance = calculateTourDistance(currentTour);
            double temperature = INITIAL_TEMPERATURE;
            while (temperature > 1) {
                for (int i = 0; i < NUM_NEIGHBORS; i++) {
                    int[] newTour = generateNeighborTour(currentTour, random);
                    double newDistance = calculateTourDistance(newTour);
                    if (acceptMove(currentDistance, newDistance, temperature, random)) {
                        currentTour = newTour;
                        currentDistance = newDistance;
                    }
                }
                temperature *= 1 - COOLING_RATE;
            }
            // Emit the best tour and its distance
            valueOut.set(currentDistance + "#" + Arrays.toString(currentTour));
            context.write(keyOut, valueOut);
        }

        private int[] generateInitialTour(Random random) {
            List<Integer> tour = new ArrayList<>();
            for (int i = 0; i < cities.size(); i++) {
                tour.add(i);
            }
            Collections.shuffle(tour, random);
            return tour.stream().mapToInt(Integer::intValue).toArray();
        }

        private int[] generateNeighborTour(int[] tour, Random random) {
            int[] newTour = tour.clone();
            int city1 = random.nextInt(cities.size());
            int city2 = random.nextInt(cities.size());
            int temp = newTour[city1];
            newTour[city1] = newTour[city2];
            newTour[city2] = temp;
            return newTour;
        }

        private double calculateTourDistance(int[] tour) {
            double distance = 0;
            for (int i = 1; i < tour.length; i++) {
                int[] city1 = cities.get(tour[i - 1]);
                int[] city2 = cities.get(tour[i]);
                distance += Math.sqrt(Math.pow(city1[0] - city2[0], 2) + Math.pow(city1[1] - city2[1], 2));
            }
            return distance;
        }

        private boolean acceptMove(double currentDistance, double newDistance, double temperature, Random random) {
            if (newDistance < currentDistance) {
                return true;
            }
            double acceptanceProbability = Math.exp((currentDistance - newDistance) / temperature);
            return random.nextDouble() < acceptanceProbability;
        }
    }

    public static class TSPReducer extends Reducer<Text, Text, Text, Text> {
        private Text valueOut = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // Find the best tour and its distance
            double bestDistance = Double.MAX_VALUE;
            String bestTourTuple = "";
            for (Text value : values) {
                String tourTuple = value.toString();
                String[] tourArgs = tourTuple.split("#");
                double distance = Double.parseDouble(tourArgs[0]);
                if (distance < bestDistance) {
                    bestDistance = distance;
                    bestTourTuple = tourTuple;
                }
            }
            // Emit the best tour and its distance
            valueOut.set(bestTourTuple);
            context.write(key, valueOut);
        }
    }
}
