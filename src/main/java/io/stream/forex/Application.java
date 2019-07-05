package io.stream.forex;

import io.stream.forex.client.QuoteClient;
import io.stream.forex.server.QuoteServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Application {
    final static Logger LOG = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {

        ExecutorService executorService = Executors.newFixedThreadPool(2);

        QuoteServer quoteServer = new QuoteServer();
        QuoteClient quoteClient = new QuoteClient();

        executorService.execute(quoteServer);

//        try {
//            executorService.awaitTermination(10, TimeUnit.SECONDS);
//        } catch (InterruptedException e) {
//            LOG.error("executorService is timeout", e);
//        }

        executorService.execute(quoteClient);

        try {
            executorService.awaitTermination(3, TimeUnit.MINUTES);
            executorService.shutdown();
        } catch (InterruptedException e) {
            LOG.error("executorService is timeout", e);
        }

    }
}
