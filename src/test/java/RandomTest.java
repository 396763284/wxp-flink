import java.util.Random;

public class RandomTest {

    public static void main(String[] args) {
        String[] str={"M","E"};
        for (int i = 0; i < 10; i++) {
            System.out.println(new Random().nextInt(2));
            //System.out.println(str[new Random().nextInt(1)]);
        }

    }
}
