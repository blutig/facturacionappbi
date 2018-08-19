package co.com.sistemafacturacion.appbi;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class AppBIApplication {

	private static String BOOTSTRAP_SERVERS = "localhost:3032";
	private static String TOPIC_BI = "bi";
	private static String GROUP_BI = "appbi";

	private static Consumer<String, String> createConsumerCompras() {
		final Properties propiedades = new Properties();
		propiedades.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		propiedades.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		propiedades.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		propiedades.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_BI);
		propiedades.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		propiedades.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

		// Create the consumer using props.
		final Consumer<String, String> consumer = new KafkaConsumer<>(propiedades);

		// Subscribe to the topic.
		consumer.subscribe(Arrays.asList(TOPIC_BI));
		return consumer;
	}

	static void executeConsumer() throws InterruptedException {
		final Consumer<String, String> consumer = createConsumerCompras();
		final int giveUp = 100;
		int noRecordsCount = 0;

		while (true) {
			ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);

			if (consumerRecords.count() == 0) {
				noRecordsCount++;
				if (noRecordsCount > giveUp)
					break;
				else
					continue;
			} else {
				noRecordsCount = 0;
			}

			for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
				System.out.println(".");
				System.out.println("Factura entrante a BI");
				System.out.println(".");
				System.out.println("Key: '" + consumerRecord.key() + "'");
				System.out.println("Value: '" + consumerRecord.value() + "'");
				System.out.println("Partition: '" + consumerRecord.partition() + "'");
				System.out.println("Topic: '" + consumerRecord.topic() + "'");
				System.out.println(".");
			}
		}
	}

	public static void main(String[] args) throws InterruptedException {
		executeConsumer();
	}
}
