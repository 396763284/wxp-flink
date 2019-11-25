package steaming.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Properties;
import java.util.Random;


public class MockKafkaProducer {

    private static Logger logger= LoggerFactory.getLogger("MockKafkaProducer");

    @SuppressWarnings("InfiniteLoopStatement")
    public static void main(String[] args) {
        String topic = "wxpflink";
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        try {
            while (true) {
                String msg = generateMsg();
                System.out.println(msg);
                kafkaProducer.send(new ProducerRecord<String, String>(topic, msg));
                Thread.sleep(2000);
            }
        } catch (InterruptedException e){
            e.printStackTrace();
        }
        finally {
            kafkaProducer.close();
        }
    }

    private static String generateMsg(){
       // "imooc cn E 2019-11-21 22:37:01 11.22.11 vv.con 2";
        StringBuilder builder= new StringBuilder();
        builder.append("imooc").append("\t");
        builder.append("cn").append("\t");
        builder.append(generateDem()).append("\t");
        builder.append(generateDate()).append("\t");
        builder.append("vv.con").append("\t");
        builder.append(new Random().nextInt(200)).append("\t");
        return builder.toString();
    }

    private static String generateDem(){
        String[] str={"M","E"};
        return str[new Random().nextInt(2)];
    }

    private static String generateDate(){
        Calendar calendar = Calendar.getInstance();
        SimpleDateFormat format=new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        String[] str={"M","E"};
        return format.format(calendar.getTime());
    }
}
