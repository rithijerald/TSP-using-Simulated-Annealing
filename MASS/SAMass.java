package edu.uwb.css534;

import edu.uw.bothell.css.dsl.MASS.*;
import edu.uw.bothell.css.dsl.MASS.Places;
import edu.uw.bothell.css.dsl.MASS.logging.LogLevel;
import edu.uw.bothell.css.dsl.MASS.matrix.MatrixUtilities;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

public class SAMass implements Serializable {
    private static final String NODE_FILE = "nodes.xml";

    private static final int NUM_PLACES_X = 10;
    private static final int NUM_PLACES_Y = 10;
    private static final double INIT_TEMPERATURE = 1000; // The cooling rate for the simulated annealing algorithm
    private static final double COOLING_RATE = 0.003; // The cooling rate for the simulated annealing algorithm

    /**
     * @return list of cities with a pair of index
     */
    private static List<int[]> readCities() {
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
        // read city list
        final List<int[]> cities = readCities();
        // init MASS library
        MASS.setNodeFilePath(NODE_FILE);
        MASS.setLoggingLevel(LogLevel.WARN);
        // Initialize MASS framework
        MASS.init();
        // start time
        long startTime = System.currentTimeMillis();
        // Create places, init with cities and random seed in Object[]
        Places places = new Places(1, TSPPlace.class.getName(), cities, NUM_PLACES_X, NUM_PLACES_Y);
//        int placeNum = places.getPlacesSize();
        int placeNum = MatrixUtilities.getMatrixSize(places.getSize());
        System.out.println("places=" + placeNum);
        // Simulated annealing algorithm
        double temp = INIT_TEMPERATURE;
        double logTemp = temp;
        double bestCost = Double.MAX_VALUE;
        List<Integer> bestSolution = null;
        while (temp > 1) {
            //get all final solution
            Object[] placeCallAllObjs = new Object[placeNum];
            Object placeArg = new Object[]{temp, bestSolution};
            for (int i = 0; i < placeNum; i++) {
                placeCallAllObjs[i] = placeArg;
            }
            Object[] solutions = (Object[]) places.callAll(TSPPlace.SET_TEMP_AND_GENERATE_SOLUTION, placeCallAllObjs);
            for (Object s : solutions) {
//                System.out.println(s.getClass().getName());
                List<Integer> solution = (List<Integer>) s;
                double cost = TSPPlace.calculateCost(solution, cities);
                if (cost < bestCost) {
                    bestSolution = solution;
                    bestCost = cost;
                    System.out.println(">>> New solution cost: " + bestCost + " solution: " + bestSolution);
                }
            }
            // Cool down
            temp *= 1 - COOLING_RATE;
            if (temp < logTemp * 0.8) {
                logTemp = temp;
                System.out.println(">>> temperature = " + temp);
            }
        }
        // Print the best solution
        System.out.println(">>> Best solution: " + bestSolution);
        System.out.println(">>> Cost: " + bestCost); 
        System.out.println(">>> Time:" + ((System.currentTimeMillis()) - startTime) );
        // Finish MASS framework
        MASS.finish();
    }
}
