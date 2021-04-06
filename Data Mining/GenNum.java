import java.io.File;  
import java.io.FileNotFoundException;  
import java.util.Scanner; 
import java.util.Arrays;
import java.util.Random;

// Usage 
// java GenNum.java 10
// gives 10 lins
// java GenNum.java x
// gives x lines

public class GenNum {
  public static void main(String[] args) {
    int max = 100000;
    int date = 100000;
    int arg1 = Integer.parseInt(args[0]);
    Random rand = new Random();
    try {
      for(int i = 0; i < arg1; i++) {
      	System.out.print("(");
        for(int j = 0; j < 20; j++) {
          if(j == 0) {
            System.out.print(rand.nextInt(2) + ", ");
          } else if (j == 1) {
            System.out.print(date + ", ");
            date += 10;
          } else {
            float ram = rand.nextInt(max) + rand.nextFloat();
            if(j == 19) {
              System.out.print(ram + ")" + "\n");
            } else {
              System.out.print(ram + ", ");
            }
          }
        }
      }

    } catch (Exception e) {
      System.out.println("An error occurred.");
    }
  }
}
