import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Scanner;

public class Experimental {

    public static void main(String[] args) throws FileNotFoundException {

        Scanner in = new Scanner(new FileReader("hdfs://localhost:8020/user/admin/matches.csv"));

        StringBuilder sb = new StringBuilder();
        while (in.hasNext()) {
            sb.append(in.next());
        }
        in.close();

        System.out.println(sb);
    }

}