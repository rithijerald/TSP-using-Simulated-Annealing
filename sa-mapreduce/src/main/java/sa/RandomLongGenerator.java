package sa;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class RandomLongGenerator {
    private static int SIZE = 10;
    // file path
    private static String OUTPUT_PATH = "./random_seed.txt";

    public static void main(String[] args) {
        Random rand = new Random(534);
        File file = new File(OUTPUT_PATH);
        try (FileWriter fw = new FileWriter(file)) {
            System.out.println("output path: " + file.getAbsolutePath());
            fw.write("");
            for (int i = 0; i < SIZE; i++) {
                fw.append(String.valueOf(rand.nextLong()));
                fw.append("\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
