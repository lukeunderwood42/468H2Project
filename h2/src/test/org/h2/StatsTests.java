package org.h2;

import org.h2.test.TestBase;
import org.h2.test.unit.TestCache;
import org.h2.util.Cache;
import org.h2.util.CacheClock;
import org.h2.util.CacheLFU;
import org.h2.util.CacheObject;
import org.h2.util.CacheLRU;
import org.h2.util.CacheRandom;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class StatsTests {

  static class Obj extends CacheObject {

    Obj(int pos) {
      setPos(pos);
    }

    @Override
    public int getMemory() {
      return 1024;
    }

    @Override
    public boolean canRemove() {
      return true;
    }

    @Override
    public boolean isChanged() {
      return true;
    }

    @Override
    public String toString() {
      return "[" + getPos() + "]";
    }

  }

  public static void main(String[] args) throws Exception {
    testAllWithInputs(500, 20, 3000);
  }

  public static void testClockLoad(Obj[] inputs, int size){
    TestCache test = new TestCache();
    CacheClock cache = new CacheClock(test, size * 4, false);
    long start = System.currentTimeMillis();
    for(int i = 0; i < inputs.length; i++){
      if(cache.get(inputs[i].getPos()) == null){
        cache.put(inputs[i]);
      }
    }
    long end = System.currentTimeMillis();
    end += cache.misses * 10L;
    System.out.printf("Time elapsed for clock: %d%n", end - start);
    System.out.printf("Hits for Clock: %d%n", cache.hits);
    System.out.printf("Misses for Clock: %d%n", cache.misses);
  }

  public static void testLFULoad(Obj[] inputs, int size){
    TestCache test = new TestCache();
    CacheLFU cache = new CacheLFU(test, size * 4);
    long start = System.currentTimeMillis();
    for(int i = 0; i < inputs.length; i++){
      if(cache.get(inputs[i].getPos()) == null){
        cache.put(inputs[i]);
      }
    }
    long end = System.currentTimeMillis();
    end += cache.misses * 10L;
    System.out.printf("Time elapsed for LFU: %d%n", end - start);
    System.out.printf("Hits for LFU: %d%n", cache.hits);
    System.out.printf("Misses for LFU: %d%n", cache.misses);
  }

  public static void testLRULoad(Obj[] inputs, int size){
    TestCache test = new TestCache();
    CacheLRU cache = new CacheLRU(test, size * 4, false);
    long start = System.currentTimeMillis();
    for(int i = 0; i < inputs.length; i++){
      if(cache.get(inputs[i].getPos()) == null){
        cache.put(inputs[i]);
      }
    }
    long end = System.currentTimeMillis();
    end += cache.misses * 10L;
    System.out.printf("Time elapsed for LRU: %d%n", end - start);
    System.out.printf("Hits for LRU: %d%n", cache.hits);
    System.out.printf("Misses for LRU: %d%n", cache.misses);
  }

  public static void testFifoLoad(Obj[] inputs, int size){
    TestCache test = new TestCache();
    CacheLRU cache = new CacheLRU(test, size * 4, true);
    long start = System.currentTimeMillis();
    for(int i = 0; i < inputs.length; i++){
      if(cache.get(inputs[i].getPos()) == null){
        cache.put(inputs[i]);
      }
    }
    long end = System.currentTimeMillis();
    end += cache.misses * 10L;
    System.out.printf("Time elapsed for FIFO: %d%n", end - start);
    System.out.printf("Hits for FIFO: %d%n", cache.hits);
    System.out.printf("Misses for FIFO: %d%n", cache.misses);
  }

  public static void testRandomLoad(Obj[] inputs, int size){
    TestCache test = new TestCache();
    CacheRandom cache = new CacheRandom(test, size * 4, true);
    long start = System.currentTimeMillis();
    for(int i = 0; i < inputs.length; i++){
      if(cache.get(inputs[i].getPos()) == null){
        cache.put(inputs[i]);
      }
    }
    long end = System.currentTimeMillis();
    end += cache.misses * 10L;
    System.out.printf("Time elapsed for Random: %d%n", end - start);
    System.out.printf("Hits for Random: %d%n", cache.hits);
    System.out.printf("Misses for Random: %d%n", cache.misses);
  }

  public static void testAllWithInputs(int range,  int cacheSize, int inputSize){
    Obj[] inputSet = generateRandomInput(range, inputSize);
    testClockLoad(inputSet, cacheSize);
    testLFULoad(inputSet, cacheSize);
    testLRULoad(inputSet, cacheSize);
    testFifoLoad(inputSet, cacheSize);
    testRandomLoad(inputSet, cacheSize);
  }

  public static Obj[] generateRandomInput(int range, int size){
    Obj[] output = new Obj[size];
    Random random = new Random();
    for(int i = 0; i < size; i++){
      int rand = random.nextInt(range);
      output[i] = new Obj(rand);
    }
    return output;
  }




}
