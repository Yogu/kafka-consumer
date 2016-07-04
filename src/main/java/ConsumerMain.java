import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import de.unistuttgart.isw.serviceorchestration.api.MessageBus;
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

		MessageBus bus = new MessageBus();
		bus.createReceiver("input", "https://opcfoundation.org/UA/2008/02/Types.xsd",
				xml -> {
					lastLog = xml;
					System.out.println(xml);
				});

		bus.runListener();
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
