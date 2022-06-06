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
        while (numberOfRuns >= 0) {
            testAllWithInputs(500, 200, 10000);
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
        Obj[] inputSet = generateRandomInput(range, inputSize);
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
