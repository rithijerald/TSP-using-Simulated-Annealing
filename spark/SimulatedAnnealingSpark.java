import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

import scala.Tuple2;

public class SimulatedAnnealingSpark implements Serializable {
    // neighbors for each round of SA
    private final static int NUM_NEIGHBORS = 100;
    // Set initial temp
    private final static double INI_TEMPERATURE = 1000;
    // Cooling rate
    private final static double COOLING_RATE = 0.003;

    /**
     * @return list of cities with a pair of index
     */
    private List<int[]> readCities() {
        // Create a list of cities and read cities from file
        String citiesFile = "cities.txt";
        File file = new File(citiesFile);
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

    public static void main(String[] args) {
        // Create a Spark Context
        SparkConf sparkConf = new SparkConf().setAppName("SimulatedAnnealingSpark")
	    .setMaster("local[2]").set("spark.executor.memory", "2g");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        // log level
        sc.setLogLevel("WARN");
        SimulatedAnnealingSpark sa = new SimulatedAnnealingSpark();
        sa.run(sc);
    }

    public void run(JavaSparkContext sc) {
        //random with seed
        Random rand = new Random(534);
        // read city list
        final List<int[]> cities = readCities();
        // start time
        long startTime = System.currentTimeMillis();
        // Initial solution
        List<Integer> currentSolution = generateInitialSolution(cities.size());
        List<Integer> bestSolution = new ArrayList<>(currentSolution);
        double bestCost = calculateCost(bestSolution, cities);
        // Set initial temp
        double temp = INI_TEMPERATURE;
        double logTemp = temp;
        // Start the SA outer loop to cool down the system
        while (temp > 1) {
            List<List<Integer>> neighbors = generateNeighbors(currentSolution, rand);
            JavaRDD<List<Integer>> neighborsRDD = sc.parallelize(neighbors, NUM_NEIGHBORS);
            List<Tuple2<List<Integer>, Double>> solutionsAndCosts = neighborsRDD
		.map(solution -> new Tuple2<>(solution, calculateCost(solution, cities)))
		.collect();
            for (Tuple2<List<Integer>, Double> solutionAndCost : solutionsAndCosts) {
                List<Integer> newSolution = solutionAndCost._1;
                double newCost = solutionAndCost._2;
                if (acceptanceProbability(calculateCost(currentSolution, cities), newCost, temp) > Math.random()) {
                    currentSolution = new ArrayList<>(newSolution);
                }
                if (newCost < bestCost) {
                    bestSolution = new ArrayList<>(newSolution);
                    bestCost = newCost;
                    System.out.println(">>> New solution cost: " + newCost + " solution: " + newSolution);
                }
            }
            temp *= 1 - COOLING_RATE;
            if (temp < logTemp * 0.8) {
                logTemp = temp;
                System.out.println(">>> temperature = " + temp);
            }
        }
        System.out.println(">>>>>>> Result >>>>");
        System.out.println(">>> Best cost: " + bestCost);
        System.out.println(">>> Best solution: " + bestSolution);
        System.out.println(">>> Total time(ms): " + (System.currentTimeMillis() - startTime));
        sc.close();
    }

    private List<Integer> generateInitialSolution(int size) {
        List<Integer> initialSolution = new ArrayList<>();
        for (int j = 0; j < size; j++) {
            initialSolution.add(j);
        }
        Collections.shuffle(initialSolution);
        return initialSolution;
    }

    private List<List<Integer>> generateNeighbors(List<Integer> currentSolution, Random rand) {
        List<List<Integer>> neighbors = new ArrayList<>();
        for (int i = 0; i < NUM_NEIGHBORS; i++) {
            List<Integer> newSolution = new ArrayList<>(currentSolution);
            int swapIndex1 = rand.nextInt(newSolution.size());
            int swapIndex2 = rand.nextInt(newSolution.size());
            Collections.swap(newSolution, swapIndex1, swapIndex2);
            neighbors.add(newSolution);
        }
        return neighbors;
    }

    private double acceptanceProbability(double currentCost, double newCost, double temp) {
        if (newCost < currentCost) {
            return 1.0;
        }
        return Math.exp((currentCost - newCost) / temp);
    }

    private double calculateCost(List<Integer> solution, List<int[]> cities) {
        double sum = 0;
        for (int i = 1; i < solution.size(); i++) {
            int[] city1 = cities.get(solution.get(i - 1));
            int[] city2 = cities.get(solution.get(i));
            sum += Math.sqrt(Math.pow(city1[0] - city2[0], 2) + Math.pow(city1[1] - city2[1], 2));
        }
        return sum;
    }
}
