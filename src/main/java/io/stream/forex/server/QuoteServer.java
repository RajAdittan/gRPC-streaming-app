package io.stream.forex.server;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.stream.forex.QuoteRequest;
import io.stream.forex.QuoteResponse;
import io.stream.forex.StreamingQuoteGrpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import yahoofinance.YahooFinance;
import yahoofinance.quotes.fx.FxQuote;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.concurrent.atomic.AtomicBoolean;

public class QuoteServer implements Runnable {
    final static Logger LOG = LoggerFactory.getLogger(QuoteServer.class);

    @Override
    public void run() {

        StreamingQuoteGrpc.StreamingQuoteImplBase service = new StreamingQuoteGrpc.StreamingQuoteImplBase() {
            @Override
            public StreamObserver<QuoteRequest> snapQuote(StreamObserver<QuoteResponse> responseObserver) {

                ServerCallStreamObserver<QuoteResponse> serverCallObserver =
                        (ServerCallStreamObserver<QuoteResponse>) responseObserver;

                final AtomicBoolean wasReady = new AtomicBoolean(false);

                serverCallObserver.setOnReadyHandler( () -> {
                    if(serverCallObserver.isReady() && wasReady.compareAndExchange(false, true)) {
                        LOG.info("QuoteServer is READY");

                        serverCallObserver.request(1);
                    }
                });

                return new StreamObserver<>() {
                    @Override
                    public void onNext(QuoteRequest quoteRequest) {
                        String quoteSymbol = quoteRequest.getSymbol();

                        BigDecimal quotePrice = getPrice(quoteSymbol);

                        QuoteResponse response = QuoteResponse.newBuilder()
                                .setSymbol(quoteSymbol)
                                .setPrice(quotePrice.doubleValue())
                                .build();
                        responseObserver.onNext(response);
                        LOG.info("response sent");
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        LOG.error("error in snapQuote", throwable);
                    }

                    @Override
                    public void onCompleted() {
                        LOG.error("snapQuote FINISH");
                    }
                };
            }
        };

        try {
            final Server server = ServerBuilder
                    .forPort(50505)
                    .addService(service)
                    .build()
                    .start();
            LOG.info("Server listening on {}", server.getPort());

            server.awaitTermination();

        } catch (IOException e) {
            LOG.error("server build has errors", e);
        } catch (InterruptedException e) {
            LOG.info("server SHUTDOWN");
        }
    }

    final static BigDecimal getPrice(String quoteSymbol) {
        try {
            FxQuote fx = YahooFinance.getFx(quoteSymbol);
            LOG.info("FxQuote received {}", fx.getPrice());
            return fx.getPrice();
        } catch (IOException e) {
            LOG.error("error getting quote price", e);
        }
        return BigDecimal.ZERO;
    }
}
