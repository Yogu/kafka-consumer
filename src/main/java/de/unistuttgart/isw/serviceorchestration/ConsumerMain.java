package de.unistuttgart.isw.serviceorchestration;

import com.siemens.ct.exi.exceptions.EXIException;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.StaxDriver;
import de.unistuttgart.isw.serviceorchestration.servicecore.MessageBus;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.Date;

public class ConsumerMain {
    private static String lastXml = "";
    private static String lastValue = "";
    private static String lastDate = "";

    public static void main(String[] args) throws IOException, EXIException, SAXException, InterruptedException {
        // opcfoundation does not like Java default user agent
        System.setProperty("http.agent", "Mozilla/5.0");
        HttpServer server = HttpServer.create(new InetSocketAddress(9000), 0);
        server.createContext("/", new MyHandler());
        server.setExecutor(null); // creates a default executor
        server.start();

        MessageBus bus = new MessageBus();
        bus.createReceiver("input", "https://opcfoundation.org/UA/2008/02/Types.xsd",
                xml -> {
                    lastXml = xml;
                    XStream xstream = new XStream(new StaxDriver());
                    xstream.alias("Float", Float.class);

                    Float value = (Float) xstream.fromXML(lastXml);

                    lastValue = value.toString();

                    lastDate= new Date().toString();
                });

        bus.runListener();

    }

    static class MyHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange t) throws IOException {
            String response = "xml: " + lastXml + "\n" + "value: " + lastValue + "\n" + "lastUpdate: " + lastDate;
            t.sendResponseHeaders(200, response.length());
            OutputStream os = t.getResponseBody();
            os.write(response.getBytes());
            os.close();
        }
    }


}
