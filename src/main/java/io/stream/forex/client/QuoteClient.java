package io.stream.forex.client;

import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.stream.forex.QuoteRequest;
import io.stream.forex.QuoteResponse;
import io.stream.forex.StreamingQuoteGrpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.ListIterator;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class QuoteClient implements Runnable {
    final static Logger LOG = LoggerFactory.getLogger(QuoteClient.class);

    @Override
    public void run() {

        ManagedChannel channel = ManagedChannelBuilder
                .forAddress("localhost", 50505)
                .usePlaintext()
                .build();

        // safe streaming only when channel is ready
        do {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                LOG.error("Sleep interrupted, no worries", e);
            }
        } while (!isChannelReady(channel));

        StreamingQuoteGrpc.StreamingQuoteStub streamingQuoteStub = StreamingQuoteGrpc.newStub(channel);
        final CountDownLatch done = new CountDownLatch(1);
        ClientResponseObserver<QuoteRequest, QuoteResponse> clientResponseObserver =
                new ClientResponseObserver<>() {

                    ClientCallStreamObserver<QuoteRequest> requestStream;

                    @Override
                    public void beforeStart(ClientCallStreamObserver<QuoteRequest> clientCallStreamObserver) {
                        this.requestStream = clientCallStreamObserver;
                        requestStream.disableAutoInboundFlowControl();
                        requestStream.setOnReadyHandler(() -> {
                            List<String> symbolsList = supportedSymbolsList();
                            ListIterator<String> listIterator = symbolsList.listIterator();
                            while(requestStream.isReady() && listIterator.hasNext()) {
                                QuoteRequest quoteRequest = QuoteRequest
                                        .newBuilder()
                                        .setSymbol(listIterator.next())
                                        .build();
                                requestStream.onNext(quoteRequest);
                                try {
                                    // delay quote request by 1 sec to get a feel
                                    // of input output streaming
                                    Thread.sleep(1000);
                                } catch (InterruptedException e) {
                                    LOG.error("Sleep interrupted, no worries", e);
                                }
                            }
                            requestStream.onCompleted();
                        });

                    }

                    @Override
                    public void onNext(QuoteResponse quoteResponse) {
                        if(Objects.isNull(quoteResponse)) {
                            LOG.warn("a null quoteResponse received, return");
                            return;
                        }
                        LOG.info("Quote: Symbol: {}, Price {}",
                                quoteResponse.getSymbol(), quoteResponse.getPrice());
                        requestStream.request(1);
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        LOG.error("An error occurred while streaming ", throwable);
                        done.countDown();
                    }

                    @Override
                    public void onCompleted() {
                        LOG.error("Streaming is completed");
                        done.countDown();
                    }
                };

        streamingQuoteStub.snapQuote(clientResponseObserver);

        try {
            done.await();
        } catch (InterruptedException e) {
            LOG.error("countdown await is interrupted, ignore", e);
        }

        channel.shutdown();
        try {
            channel.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOG.error("channel awaitTermination throws interrupted exception", e);
        }
    }

    private static boolean isChannelReady(ManagedChannel channel) {
        ConnectivityState channelState = channel.getState(true);
        AtomicBoolean isReady = new AtomicBoolean(false);
        switch (channelState) {
            case CONNECTING:
            {
                LOG.info("connecting to localhost:50505");
            }
            break;
            case READY:
            {
                isReady.compareAndExchange(false, true);
                LOG.info("connection is ready!");
            }
            break;
            case TRANSIENT_FAILURE:
            {
                LOG.error("channel has transient failure");
            }
            break;
            case IDLE:
            {
                isReady.compareAndExchange(false, true);
                LOG.info("connection is idle");
            }
            break;
            case SHUTDOWN:
            {
                LOG.warn("check channel might be shutdown ..");
                if(channel.isShutdown()) {
                    LOG.error("channel is shutdown, exit application");
                    System.exit(-1);
                }
            }
            break;
            default:{
                LOG.error("unknown channel state");
            }
        }

        if(!isReady.get()) {
            LOG.error("server not ready");
        } else {
            LOG.info("server is ready!");
        }
        return isReady.get();
    }

    private static List<String> supportedSymbolsList() {
        return List.of(
                "USDGBP=X", "USDEUR=X", "USDAUD=X", "USDCHF=X","USDJPY=X","USDCAD=X",
        "USDSGD=X", "USDNZD=X", "USDHKD=X", "GBPUSD=X", "GBPEUR=X", "GBPAUD=X", "GBPCHF=X",
        "GBPJPY=X", "GBPCAD=X", "GBPSGD=X", "GBPNZD=X", "GBPHKD=X", "EURUSD=X", "EURGBP=X",
        "EURAUD=X", "EURCHF=X", "EURJPY=X", "EURCAD=X", "EURSGD=X", "EURNZD=X", "EURHKD=X",
        "AUDUSD=X", "AUDGBP=X", "AUDEUR=X", "AUDCHF=X", "AUDJPY=X", "AUDCAD=X", "AUDSGD=X",
        "AUDNZD=X", "AUDHKD=X", "CHFGBP=X", "CHFEUR=X", "CHFAUD=X", "CHFJPY=X", "CHFCAD=X",
        "CHFSGD=X", "CHFNZD=X", "CHFHKD=X", "JPYUSD=X", "JPYGBP=X", "JPYEUR=X", "JPYAUD=X",
        "JPYCHF=X", "JPYCAD=X", "JPYSGD=X", "JPYNZD=X", "JPYHKD=X", "CADUSD=X", "CADGBP=X",
        "CADEUR=X", "CADAUD=X", "CADCHF=X", "CADJPY=X", "CADSGD=X", "CADNZD=X", "CADHKD=X",
        "SGDUSD=X", "SGDGBP=X", "SGDEUR=X", "SGDAUD=X", "SGDCHF=X", "SGDJPY=X", "SGDCAD=X",
        "SGDNZD=X", "SGDHKD=X", "NZDUSD=X", "NZDGBP=X", "NZDEUR=X", "NZDAUD=X", "NZDCHF=X",
        "NZDJPY=X", "NZDCAD=X", "NZDSGD=X", "NZDHKD=X", "HKDUSD=X", "HKDGBP=X", "HKDEUR=X",
        "HKDAUD=X", "HKDCHF=X", "HKDJPY=X", "HKDCAD=X", "HKDSGD=X", "HKDNZD=X"
        );
    }
}
