package it.unipi.dii.hadoop;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;

public class Utils {
    public static final List<Character> LETTERS =
            Arrays.asList('a', 'b', 'c', 'd', 'e',
                    'f', 'g', 'h', 'i', 'j', 'k', 'l',
                    'm', 'n', 'o', 'p', 'q', 'r', 's',
                    't', 'u', 'v', 'w', 'x', 'y', 'z');

    public static long readLetterCountValue(FileSystem fs, Path configPath) throws IOException {
        try (FSDataInputStream inputStream = fs.open(configPath);
             BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("COUNT")) {
                    // Extract value after "letter_count"
                    String[] parts = line.split("\\s+");
                    if (parts.length == 2) {
                        return Long.parseLong(parts[1]);
                    } else {
                        System.err.println("Invalid format in the output file: " + line);
                        return -1;
                    }
                }
            }
            // If "COUNT" line not found
            System.err.println("'COUNT' value not found in the output file file");
            return -1;
        }
    }
}