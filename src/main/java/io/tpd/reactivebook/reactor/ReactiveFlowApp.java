package io.tpd.reactivebook.reactor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;

public class ReactiveFlowApp {

  private static final int NUMBER_OF_MAGAZINES = 20;
  private static final long MAX_SECONDS_TO_KEEP_IT_WHEN_NO_SPACE = 2;
  private static final Logger log =
    LoggerFactory.getLogger(ReactiveFlowApp.class);

  public static void main(String[] args) throws Exception {
    final ReactiveFlowApp app = new ReactiveFlowApp();

//    log.info("\n\n### CASE 1: Subscribers are fast, buffer size is not so " +
//      "important in this case.");
//    app.magazineDeliveryExample(100L, 100L, 8);
//
//    log.info("\n\n### CASE 2: A slow subscriber, but a good enough buffer " +
//      "size on the publisher's side to keep all items until they're picked up");
//    app.magazineDeliveryExample(1000L, 3000L, NUMBER_OF_MAGAZINES);

    log.info("\n\n### CASE 3: A slow subscriber, and a very limited buffer " +
      "size on the publisher's side so it's important to keep the slow " +
      "subscriber under control");
    app.magazineDeliveryExample(1000L, 3000L, 8);

  }

  void magazineDeliveryExample(final long sleepTimeJack,
                               final long sleepTimePete,
                               final int maxStorageInPO) throws Exception {

    final ConnectableFlux<Integer> publisher = Flux.range(1, 20)
      .delayElements(Duration.ofSeconds(1))
      .onBackpressureDrop(dropped -> log.error("Dropped! " + dropped))
      .replay(Duration.ofSeconds(4));

    final MagazineSubscriber jack = new MagazineSubscriber(
      sleepTimeJack,
      MagazineSubscriber.JACK
    );
    final MagazineSubscriber pete = new MagazineSubscriber(
      sleepTimePete,
      MagazineSubscriber.PETE
    );

    log.info("Printing 20 magazines per subscriber, with room in publisher for "
      + maxStorageInPO + ". They have " + MAX_SECONDS_TO_KEEP_IT_WHEN_NO_SPACE +
      " seconds to consume each magazine.");

    publisher
      .publishOn(Schedulers.newSingle("jack"))
      .cache(maxStorageInPO, Duration.ofSeconds(MAX_SECONDS_TO_KEEP_IT_WHEN_NO_SPACE))
      .subscribe(jack);
    publisher
      .publishOn(Schedulers.newSingle("pete"))
      .doOnError(t -> System.out.println("Error! " + t.toString()))
      .subscribe(pete);

    publisher.connect();

    // Blocks until all subscribers are done (this part could be improved
    // with latches, but this way we keep it simple)


    // Closes the publisher, calling the onComplete() method on every subscriber

    // give some time to the slowest consumer to wake up and notice
    // that it's completed

  }

  private static void log(final String message) {
    log.info("===========> " + message);
  }

}
