package sa;

import mpi.MPI;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

public class SAParallel {
    private static double INITIAL_TEMP = 1000.0;
    private static final double COOLING_RATE = 0.003;
    private static final int NEIGHBORS = 100;

    private List<Integer[]> cities;

    /**
     * @param args [0] temperature [1] cities.txt
     */
    public static void main(String[] args) {
        INITIAL_TEMP = Double.parseDouble(args[3]);
        File file = new File(args[4]);
        MPI.Init(args);
        SAParallel sa = new SAParallel();
        sa.run(file);
        MPI.Finalize();
    }

    private List<Integer[]> readCities(File file) {
        // Create a list of cities and read cities from file
        List<Integer[]> cities = new ArrayList<>();
        try (Scanner scanner = new Scanner(new FileInputStream(file))) {
            while (scanner.hasNext()) {
                String line = scanner.next();
                if (!line.isBlank()) {
                    String[] lineArgs = line.split("=");
                    String[] positions = lineArgs[1].split(",");
                    cities.add(new Integer[]{Integer.parseInt(positions[0]), Integer.parseInt(positions[1])});
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Read " + cities.size() + " cities from:" + file.getPath());
        return cities;
    }

    public void run(File file) {
        long startTime = System.currentTimeMillis();
        int rank = MPI.COMM_WORLD.Rank();
        int numProcesses = MPI.COMM_WORLD.Size();
        //random based on rank as seed
        Random rand = new Random(rank);
        cities = readCities(file);
        double temp = INITIAL_TEMP, logTmp = INITIAL_TEMP;
        List<Integer> currentSolution = generateInitialSolution(rand);
        List<Integer> bestSolution = new ArrayList<>(currentSolution);
        double bestCost = calculateCost(currentSolution);
        while (temp > 1) {
            for (int i = 0; i < NEIGHBORS; i++) {
                List<Integer> newSolution = generateNeighborSolution(currentSolution, rand);
                // Calculate the costs for current and new solutions
                double currentCost = calculateCost(currentSolution);
                double newCost = calculateCost(newSolution);
                // Perform local updates on each process
                if (acceptanceProbability(currentCost, newCost, temp) > Math.random()) {
                    currentSolution = new ArrayList<>(newSolution);
                }
                // Determine the local best solution and cost
                if (newCost < bestCost) {
                    currentSolution = newSolution;
                    bestCost = newCost;
                }
            }
            // Synchronize the best solution and cost across all processes
            double[] globalBestCostBuff = new double[]{bestCost};
            int bestRoot = 0;
            double globalBestCost = bestCost;
//            System.out.println(">>> rank[" + rank + "] best cost =" + bestCost);
            // get best cost & best root
            if (rank == 0) {
                for (int i = 1; i < numProcesses; i++) {
                    MPI.COMM_WORLD.Recv(globalBestCostBuff, 0, 1, MPI.DOUBLE, i, 0);
                    if (globalBestCostBuff[0] < globalBestCost) {
                        bestRoot = i;
                        globalBestCost = globalBestCostBuff[0];
                    }
                }
                globalBestCostBuff[0] = bestRoot;
                for (int i = 1; i < numProcesses; i++) {
                    MPI.COMM_WORLD.Send(globalBestCostBuff, 0, 1, MPI.DOUBLE, i, 0);
                }
            } else {
                MPI.COMM_WORLD.Send(globalBestCostBuff, 0, 1, MPI.DOUBLE, 0, 0);
                MPI.COMM_WORLD.Recv(globalBestCostBuff, 0, 1, MPI.DOUBLE, 0, 0);
            }
            bestRoot = (int) globalBestCostBuff[0];
//            System.out.println("<<< rank[" + rank + "] best root =" + bestRoot);
            // update bestSolution from bestRoot
            int[] tempBestSolution = new int[currentSolution.size()];
            for (int i = 0; i < currentSolution.size(); i++) {
                tempBestSolution[i] = currentSolution.get(i);
            }
            MPI.COMM_WORLD.Bcast(tempBestSolution, 0, tempBestSolution.length, MPI.INT, bestRoot);
            for (int i = 0; i < currentSolution.size(); i++) {
                currentSolution.set(i, tempBestSolution[i]);
            }
            if (rank == 0 && bestCost > calculateCost(currentSolution)) {
                bestSolution = currentSolution;
                System.out.println(">>> New best solution cost=" + bestCost + "\t tmp=" + temp);
            }
//            System.out.println("<<< rank[" + rank + "] best currentSolution =" + currentSolution);
            // Update the temperature
            temp *= 1 - COOLING_RATE;
            if (temp < logTmp * 0.8) {
                logTmp = temp;
                System.out.println(">>> Rank[" + rank + "] tmp=" + temp);
            }
        }
        // Print the best solution and cost from process 0
        if (rank == 0) {
            System.out.println(">>> Best cost: " + bestCost);
            System.out.println(">>> Best solution: " + bestSolution);
            System.out.println(">>> Time: " + (System.currentTimeMillis() - startTime));
        }
    }

    private List<Integer> generateInitialSolution(Random rand) {
        List<Integer> initialSolution = new ArrayList<>();
        for (int i = 0; i < cities.size(); i++) {
            initialSolution.add(i);
        }
        Collections.shuffle(initialSolution, rand);
        return initialSolution;
    }

    private List<Integer> generateNeighborSolution(List<Integer> currentSolution, Random rand) {
        List<Integer> newSolution = new ArrayList<>(currentSolution);
        int swapIndex1 = (int) (newSolution.size() * rand.nextDouble());
        int swapIndex2 = (int) (newSolution.size() * rand.nextDouble());
        Collections.swap(newSolution, swapIndex1, swapIndex2);
        return newSolution;
    }

    private double calculateCost(List<Integer> solution) {
        double sum = 0;
        for (int i = 1; i < solution.size(); i++) {
            Integer[] city1 = cities.get(solution.get(i - 1));
            Integer[] city2 = cities.get(solution.get(i));
            sum += Math.sqrt(Math.pow(city1[0] - city2[0], 2) + Math.pow(city1[1] - city2[1], 2));
        }
        return sum;
    }

    private double acceptanceProbability(double currentCost, double newCost, double temp) {
        if (newCost < currentCost) {
            return 1.0;
        }
        return Math.exp((currentCost - newCost) / temp);
    }
}