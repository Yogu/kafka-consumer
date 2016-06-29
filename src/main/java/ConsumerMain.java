import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ConsumerMain {
	private static String lastLog = "";

	public static void main(String[] args) throws IOException {
		System.out.println("Starting http server...");
		HttpServer server = HttpServer.create(new InetSocketAddress(9000), 0);
		server.createContext("/", new MyHandler());
		server.setExecutor(null); // creates a default executor
		server.start();
		System.out.println("Listening on port 9000");

		if (System.getenv("INPUT_input") == null) {
			throw new RuntimeException("Missing environment variable INPUT_input");
		}

		Properties props = new Properties();
		props.put("bootstrap.servers", "kafka:9092");
		props.put("group.id", "test");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		System.out.println("Creating consumer...");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		System.out.println("Created consumer");
		for (Map.Entry<String, List<PartitionInfo>> topic : consumer.listTopics().entrySet()) {
			System.out.println("  topic " + topic.getKey() + " with " + topic.getValue().size() + " partitions: ");
			for (PartitionInfo partition : topic.getValue()) {
				System.out.println("    partition " + partition.partition() + ", leader: " + partition.leader().host());
			}
		}

		String[] topicNames = System.getenv("INPUT_input").split(",");
		System.out.println("Subscribing to " + System.getenv("INPUT_input"));
		consumer.subscribe(Arrays.asList(topicNames));
		System.out.println("Waiting for messages...");

		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records) {
				lastLog = String.format("offset = %d, key = %s, value = %s", record.offset(),
						record.key(),
						record.value());
				System.out.println(lastLog);
			}
		}
	}

	static class MyHandler implements HttpHandler {
		@Override
		public void handle(HttpExchange t) throws IOException {
			String response = lastLog;
			t.sendResponseHeaders(200, response.length());
			OutputStream os = t.getResponseBody();
			os.write(response.getBytes());
			os.close();
		}
	}
}
