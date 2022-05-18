package org.h2.test;

import org.h2.test.unit.TestCache;
import org.h2.util.CacheClock;
import org.h2.util.CacheObject;

import java.util.*;

public class TestBuffer extends TestBase {

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

  public static void main(String... a) throws Exception {
    TestBase.createCaller().init().testFromMain();
  }

  @Override
  public void test() throws Exception {
   testFIFO();
   testLRU(3);
   testClock();
   testLFU();
  }

  private void testFIFO(){
    ArrayList<Integer> expected = new ArrayList<>(Arrays.asList(3, 4, 5));
    ArrayList<Integer> input =  new ArrayList<>(Arrays.asList(1, 2, 3, 4, 5));
    ArrayList<Integer> actual =  new ArrayList<>();
    Integer bufferSize = 3; // for now
    //replace with actual fifo logic
    FifoLogic(actual, input, bufferSize);
    assertEquals(expected, actual);
  }

  private void testLRU(int capacity) {
    ArrayList<Integer> expected = new ArrayList<>(Arrays.asList(1,2,3));
    ArrayList<Integer> input = new ArrayList<>(Arrays.asList(1,2,3,5,2,6, 3,2,1));
    ArrayList<Integer> actual = new ArrayList<>();

    Deque<Integer> doublyQueue;
    HashSet<Integer> hashSet;
    int CACHE_SIZE;

    doublyQueue = new LinkedList<>();
    hashSet = new HashSet<>();
    CACHE_SIZE = capacity;

    for (int i = 0; i < input.size(); i++) {

      int page = input.get(i);

      if (!hashSet.contains(page)){
        if(doublyQueue.size() == CACHE_SIZE){
          int last = doublyQueue.removeLast();
          hashSet.remove(last);
        }
    } else{
        doublyQueue.remove(page);
      }
      doublyQueue.push(page);
      hashSet.add(page);
  }

    actual.addAll(doublyQueue);


    //replace with actual LRU logic
    //LRULogic(actual, input)
    assertEquals(expected, actual);
  }

  private void testClock(){
    ArrayList<Item> expected = new ArrayList<>(Arrays.asList(new Item(2, 1),
      new Item(4, 1), new Item(5, 0)));
    ArrayList<Integer> input =  new ArrayList<>(Arrays.asList(3, 4, 5, 2, 4));
    ArrayList<Item> actual =  new ArrayList<>();
    clockLogic(actual, input, 3);
    assertEquals(expected, actual);

    TestCache test = new TestCache();
    CacheClock cache = new CacheClock(test, 15, false);
    Obj object = new Obj(1);
    Obj object2 = new Obj(3);
    Obj object3 = new Obj(5);
    Obj object4 = new Obj(2);
    Obj object5 = new Obj(6);
    Obj object6 = new Obj(7);
    Obj object7 = new Obj(8);
    Obj object8 = new Obj(9);
    Obj object9 = new Obj(4);
    Obj object10 = new Obj(10);
    cache.put(object);
    cache.put(object2);
    cache.put(object3);
    cache.put(object4);
    cache.put(object5);
//    cache.put(object6);
//    cache.put(object7);
//    cache.put(object8);
//    cache.put(object9);
//    cache.put(object10);

    assertEquals(object3, cache.find(5));
    assertEquals(object2, cache.find(3));
  }

  private void testLFU(){
    ArrayList<Item> expected = new ArrayList<>(Arrays.asList(new Item(4, 1), new Item(2, 4),
      new Item(1, 4)
      ));
    ArrayList<Integer> input =  new ArrayList<>(Arrays.asList(1, 2, 3, 1, 1, 2, 2, 3, 4));
    ArrayList<Item> actual =  new ArrayList<>();
    //replace with actual clock logic
    LFUlogic(actual, input, 3);
    assertEquals(expected, actual);
  }

  private class Item{
    public int flag;
    public int value;

    public Item(int valueIn, int flagIn){
      flag = flagIn;
      value = valueIn;
    }

    public int getFlag() {
      return flag;
    }

    @Override
    public boolean equals(Object o){
      if (o == this) {
        return true;
      }


      if (!(o instanceof Item)) {
        return false;
      }

      // typecast o to Complex so that we can compare data members
      Item c = (Item) o;

      // Compare the data members and return accordingly
      return Double.compare(value, c.value) == 0
        && Double.compare(flag, c.flag) == 0;
    }

  }

  // running thru fifo logic 
  private static void FifoLogic(ArrayList<Integer> actual, ArrayList<Integer> input, Integer bufferSize) {
    // loop thru the input array
    for (Integer i : input) {
      // exceeded buffer size so remove the first
      if(actual.size() >= bufferSize) {
        actual.remove(0);
      }
      // add the next item to the actual list
      actual.add(i);
    }
  }

  private void clockLogic(ArrayList<Item> actual, ArrayList<Integer> input, int bufferSize){
    int clockPosition = 0;
    boolean inserted = false;
    boolean alreadyIn = false;
    for(Integer i: input){
      Item toBeAdded = new Item(i, 1);

      for(Item item: actual) {
        if(item.value == toBeAdded.value){
          item.flag = 1;
          alreadyIn = true;
        }
      }

      if(actual.size() < bufferSize && !alreadyIn){
        actual.add(toBeAdded);
      }

      else if (!alreadyIn){
        while(!inserted) {

          if(actual.get(clockPosition).flag == 0){
            actual.set(clockPosition, toBeAdded);
            inserted = true;
          }
          else{
            actual.get(clockPosition).flag = 0;
            clockPosition ++;
            if(clockPosition == 3){
              clockPosition = 0;
            }
          }
        }
      }
    }
  }

  private void LFUlogic(ArrayList<Item> actual, ArrayList<Integer> input, Integer bufferSize) {
    for (Integer val : input) {
      actual.sort(Comparator.comparing(Item::getFlag));
      boolean inserted = false;
      for(Item i: actual){
        if(i.value == val){
          i.flag *= 2;
          inserted = true;
        }
      }
      if (actual.size() < bufferSize && !inserted) {
        actual.add(new Item(val, 1));

        inserted = true;
      }
      if (!inserted){
        actual.remove(0);
        actual.add( new Item(val, 1));
      }
    }
    actual.sort(Comparator.comparing(Item::getFlag));
  }

}
