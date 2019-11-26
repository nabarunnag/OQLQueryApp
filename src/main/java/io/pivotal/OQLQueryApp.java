package io.pivotal;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.pdx.ReflectionBasedAutoSerializer;

public class OQLQueryApp {
  public static void main(String[] args) {
    Properties properties = new Properties();
    String statsFile = new File("home/vcap/logs/stats.gfs").getAbsolutePath();
    properties.setProperty("enable-time-statistics", "true");
    properties.setProperty("log-level", "config");
    properties.setProperty("statistic-sampling-enabled", "true");
    properties.setProperty("member-timeout", "8000");
    properties.setProperty("security-client-auth-init", "io.pivotal.ClientAuthInitialize.create");

    ClientCacheFactory ccf = new ClientCacheFactory(properties);
    ccf.setPdxSerializer(new ReflectionBasedAutoSerializer("benchmark.geode.data.*"));
    ccf.set("statistic-archive-file", statsFile);
    try {
      List<URI> locatorList = EnvParser.getInstance().getLocators();
      for (URI locator : locatorList) {
        ccf.addPoolLocator(locator.getHost(), locator.getPort());
      }
      ClientCache clientCache = ccf.create();
      final int numThreads = Runtime.getRuntime().availableProcessors();
      final ExecutorService threadPool = Executors.newFixedThreadPool(numThreads);
      final List<CompletableFuture<Void>> futures = new ArrayList<>();
      for (int i = 0; i < numThreads; i++) {
        futures.add(CompletableFuture.runAsync(() -> {
          QueryService queryService = clientCache.getQueryService();
          Query query = queryService.newQuery("SELECT * FROM /region r WHERE r.ID >= $1 AND r.ID < $2");
          while(true){
            long minID = ThreadLocalRandom.current().nextLong(1, 9999 - 10);
            long maxID = minID + 10;
            try {
              SelectResults results = (SelectResults) query.execute(minID, maxID);
              System.out.println("NABA:: " + results.asList().get(0));
            } catch (Exception e) {
              e.printStackTrace();
            }
          }
        }));
      }
      futures.forEach(CompletableFuture::join);

    }catch (IOException | URISyntaxException e){
      throw new RuntimeException("Could not deploy application", e);
    }
    try {
      Thread.sleep(Long.MAX_VALUE);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

  }
}
