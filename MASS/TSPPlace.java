package edu.uwb.css534;

import edu.uw.bothell.css.dsl.MASS.Place;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class TSPPlace extends Place {
    // neighbors for each round of SA
    private final static int NUM_NEIGHBORS = 100;

    public static final int SET_TEMP_AND_GENERATE_SOLUTION = 1; // A call to set a new temperature and generate solution
    private final List<int[]> cities; // The distances between cities
    private final Random rand;
    private List<Integer> currentSolution; // The current solution

    public TSPPlace(Object obj) {
        super();
        cities = (List<int[]>) obj;
        // random seed from current Place's index
        long randSeed = (long) getIndex()[1] * Integer.MAX_VALUE + getIndex()[0];
        System.out.println("init place randSeed=" + randSeed);
        rand = new Random(randSeed);
        currentSolution = initializeSolution(cities.size());
    }

    // Initialize solution with a simple path
    private List<Integer> initializeSolution(int size) {
        List<Integer> solution = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            solution.add(i);
        }
        Collections.shuffle(solution, rand);
        return solution;
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

    // Calculate the cost of a solution
    static double calculateCost(List<Integer> solution, List<int[]> cities) {
        double sum = 0;
        for (int i = 1; i < solution.size(); i++) {
            int[] city1 = cities.get(solution.get(i - 1));
            int[] city2 = cities.get(solution.get(i));
            sum += Math.sqrt(Math.pow(city1[0] - city2[0], 2) + Math.pow(city1[1] - city2[1], 2));
        }
        return sum;
    }

    // The call method handles all calls from the Places object
    @Override
    public Object callMethod(int methodId, Object args) {
//        System.out.println("callMethod id=" + methodId + ",args=" + args);
        switch (methodId) {
            case SET_TEMP_AND_GENERATE_SOLUTION:
                // temperature
                double temp = (double) ((Object[]) args)[0];
                List<Integer> solution = (List<Integer>) ((Object[]) args)[1];
                if (solution != null) {
                    currentSolution = solution;
                }
                List<List<Integer>> newSolutions = generateNeighbors(currentSolution, rand);
                for (List<Integer> newSolution : newSolutions) {
                    double newCost = calculateCost(newSolution, cities);
                    double currentCost = calculateCost(currentSolution, cities);
                    if (acceptanceProbability(currentCost, newCost, temp) > rand.nextDouble()) {
                        currentSolution = newSolution;
                    }
                }
                return currentSolution;
            default:
                return null;
        }
    }
}
