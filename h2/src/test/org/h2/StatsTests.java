package org.h2;

import org.h2.test.unit.TestCache;
import org.h2.util.*;

import java.util.Random;

public class StatsTests {

    static long[] ClockData = new long[3];
    static long[] LFUData = new long[3];
    static long[] LRUData = new long[3];
    static long[] FIFOData = new long[3];
    static long[] RandomData = new long[3];

    public static void main(String[] args) throws Exception {

        int numberOfRuns = 10;
        int total = numberOfRuns;
        while (numberOfRuns > 0) {


            testAllWithInputs(20, 20, 10000); // test with 2 4 6 8
            numberOfRuns--;


        }

         System.out.printf("Average Clock time elapsed " +  total +  " runs : %d%n", ClockData[0] / total);
         System.out.printf("Average Clock hits : %d%n", ClockData[1] / total);
         System.out.printf("Average Clock misses : %d%n", ClockData[2] / total);


         System.out.printf("Average LFU time elapsed "  +  total + " runs : %d%n", LFUData[0] / total);
         System.out.printf("Average LFU hits : %d%n", LFUData[1] / total);
         System.out.printf("Average LFU misses : %d%n", LFUData[2] / total);

         System.out.printf("Average LRU time elapsed "   +  total +  " runs : %d%n", LRUData[0] / total);
         System.out.printf("Average LRU hits : %d%n", LRUData[1] / total);
         System.out.printf("Average LRU misses : %d%n", LRUData[2] / total);

         System.out.printf("Average FIFO time elapsed "  +  total + " runs : %d%n", FIFOData[0] / total);
         System.out.printf("Average FIFO hits : %d%n", FIFOData[1] / total);
         System.out.printf("Average FIFO misses : %d%n", FIFOData[2] / total);

         System.out.printf("Average Random time elapsed "  +  total + " runs : %d%n", RandomData[0] / total);
         System.out.printf("Average Random hits : %d%n", RandomData[1] / total);
         System.out.printf("Average Random misses : %d%n", RandomData[2] / total);

    }

    public static void testClockLoad(Obj[] inputs, int size) {

        TestCache test = new TestCache();
        CacheClock cache = new CacheClock(test, size * 4, false);
        long start = System.currentTimeMillis();
        for (int i = 0; i < inputs.length; i++) {
            if (cache.get(inputs[i].getPos()) == null) {
                cache.put(inputs[i]);
            }
        }

        long end = System.currentTimeMillis();
        end += cache.misses * 10L;
    /*System.out.printf("Time elapsed for clock: %d%n", end - start);
    System.out.printf("Hits for Clock: %d%n", cache.hits);
    System.out.printf("Misses for Clock: %d%n", cache.misses);*/

        ClockData[0] += end - start;
        ClockData[1] += cache.hits;
        ClockData[2] += cache.misses;
    }

    public static void testLFULoad(Obj[] inputs, int size) {
        TestCache test = new TestCache();
        CacheLFU cache = new CacheLFU(test, size * 4);
        long start = System.currentTimeMillis();
        for (int i = 0; i < inputs.length; i++) {
            if (cache.get(inputs[i].getPos()) == null) {
                cache.put(inputs[i]);
            }
        }
        long end = System.currentTimeMillis();
        end += cache.misses * 10L;
    /*System.out.printf("Time elapsed for LFU: %d%n", end - start);
    System.out.printf("Hits for LFU: %d%n", cache.hits);
    System.out.printf("Misses for LFU: %d%n", cache.misses);*/

        LFUData[0] += end - start;
        LFUData[1] += cache.hits;
        LFUData[2] += cache.misses;
    }

    public static void testLRULoad(Obj[] inputs, int size) {
        TestCache test = new TestCache();
        CacheLRU cache = new CacheLRU(test, size * 4, false);
        long start = System.currentTimeMillis();
        for (int i = 0; i < inputs.length; i++) {
            if (cache.get(inputs[i].getPos()) == null) {
                cache.put(inputs[i]);
            }
        }
        long end = System.currentTimeMillis();
        end += cache.misses * 10L;
    /*System.out.printf("Time elapsed for LRU: %d%n", end - start);
    System.out.printf("Hits for LRU: %d%n", cache.hits);
    System.out.printf("Misses for LRU: %d%n", cache.misses);*/

        LRUData[0] += end - start;
        LRUData[1] += cache.hits;
        LRUData[2] += cache.misses;

    }

    public static void testFifoLoad(Obj[] inputs, int size) {
        TestCache test = new TestCache();
        CacheLRU cache = new CacheLRU(test, size * 4, true);
        long start = System.currentTimeMillis();
        for (int i = 0; i < inputs.length; i++) {
            if (cache.get(inputs[i].getPos()) == null) {
                cache.put(inputs[i]);
            }
        }
        long end = System.currentTimeMillis();
        end += cache.misses * 10L;
    /*System.out.printf("Time elapsed for FIFO: %d%n", end - start);
    System.out.printf("Hits for FIFO: %d%n", cache.hits);
    System.out.printf("Misses for FIFO: %d%n", cache.misses);*/

        FIFOData[0] += end - start;
        FIFOData[1] += cache.hits;
        FIFOData[2] += cache.misses;
    }

    public static void testRandomLoad(Obj[] inputs, int size) {
        TestCache test = new TestCache();
        CacheRandom cache = new CacheRandom(test, size * 4, true);
        long start = System.currentTimeMillis();
        for (int i = 0; i < inputs.length; i++) {
            if (cache.get(inputs[i].getPos()) == null) {
                cache.put(inputs[i]);
            }
        }
        long end = System.currentTimeMillis();
        end += cache.misses * 10L;
   /* System.out.printf("Time elapsed for Random: %d%n", end - start);
    System.out.printf("Hits for Random: %d%n", cache.hits);
    System.out.printf("Misses for Random: %d%n", cache.misses);*/

        RandomData[0] += end - start;
        RandomData[1] += cache.hits;
        RandomData[2] += cache.misses;
    }

    public static void testAllWithInputs(int range, int cacheSize, int inputSize) {
//         Obj[] inputSet = generateRandomInput(range, inputSize);
//        Obj[] inputSet = generateSkewedInput(inputSize);
//        Obj[] inputSet = generateLessSkewedInput(inputSize);
        Obj[] inputSet = generateIntervalInput(inputSize);

        testClockLoad(inputSet, cacheSize);
        testLFULoad(inputSet, cacheSize);
        testLRULoad(inputSet, cacheSize);
        testFifoLoad(inputSet, cacheSize);
        testRandomLoad(inputSet, cacheSize);
    }

    public static Obj[] generateRandomInput(int range, int size) {
        Obj[] output = new Obj[size];
        Random random = new Random();
        for (int i = 0; i < size; i++) {
            int rand = random.nextInt(range);
            output[i] = new Obj(rand);
        }
        return output;
    }

    public static Obj[] generateSkewedInput(int size) {
      Obj[] output = new Obj[size];
      Random random = new Random();
      for (int i = 0; i < size; i++) {
          int rand = random.nextInt(100);

          if(rand <= 40) {                  // 40%
            output[i] = new Obj(1);
          }
          else if(rand < 60 ) {             // 20%
            output[i] = new Obj(2);
          }
          else if(rand < 75 ) {             // 15%
            output[i] = new Obj(3);
          }
          else if(rand < 84 ) {             // 9%
            output[i] = new Obj(4);
          }
          else if(rand < 90 ) {             // 6%
            output[i] = new Obj(5);
          }
          else if(rand < 93 ) {             // 3%
            output[i] = new Obj(6);
          }
          else if(rand < 95 ) {             // 2%
            output[i] = new Obj(7);
          }
          else if(rand < 97 ) {             // 2%
            output[i] = new Obj(8);
          }
          else if(rand < 98 ) {             // 1%
            output[i] = new Obj(9);
          }
          else {                            // 1%
            output[i] = new Obj(10);
          }
      }
      return output;
  }
    public static Obj[] generateLessSkewedInput(int size) {
        Obj[] output = new Obj[size];
        Random random = new Random();
        for (int i = 0; i < size; i++) {
            int rand = random.nextInt(100);

            if(rand <= 67) {                  // 66% (0-9)
                output[i] = new Obj(rand % 10);
            }
            else {                            // 34% (10-19)
                output[i] = new Obj((rand % 10) + 10);
            }
        }
        return output;
    }



    public static Obj[] generateIntervalInput(int size) {
        Obj[] output = new Obj[size];
        Random random = new Random();

        // skewed input
        for (int i = 0; i < size / 3; i++) {
            int rand = random.nextInt(100);

            if(rand <= 80) {                                // 80% (0-9)
                output[i] = new Obj(rand % 10);
            }
            else {                                          // 20% (10-19)
                output[i] = new Obj((rand % 10) + 10);
            }

        }

        // opposite skewed input
        for (int i = size / 3; i < (2 * size / 3); i++) {
            int rand = random.nextInt(100);

            if(rand <= 20) {                                // 20% (0-9)
                output[i] = new Obj(rand % 10);
            }
            else {                                          // 80% (10-19)
                output[i] = new Obj((rand % 10) + 10);
            }
        }

        // even input
        for (int i = (2 * size / 3); i < size; i++) {
            int rand = random.nextInt(100);
            output[i] = new Obj(rand % 20);
        }

        return output;
    }

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


}
