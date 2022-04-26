package org.h2.test;

import java.util.ArrayList;
import java.util.Arrays;

public class TestBuffer extends TestBase {
  public static void main(String... a) throws Exception {
    TestBase.createCaller().init().testFromMain();
  }

  @Override
  public void test() throws Exception {
   testFIFO();
   testLRU();
   testClock();
   testLFU();
  }

  private void testFIFO(){
    ArrayList<Integer> expected = new ArrayList<>(Arrays.asList(5, 4, 3));
    ArrayList<Integer> input =  new ArrayList<>(Arrays.asList(1, 2, 3, 4, 5));
    ArrayList<Integer> actual =  new ArrayList<>();

    //replace with actual fifo logic
    //FifoLogic(actual, input);
    assertEquals(expected, actual);
  }

  private void testLRU(){
    ArrayList<Integer> expected = new ArrayList<>(Arrays.asList(3, 2, 5));
    ArrayList<Integer> input =  new ArrayList<>(Arrays.asList(3, 5, 2, 3));
    ArrayList<Integer> actual =  new ArrayList<>();
    //replace with actual LRU logic
    //LRULogic(actual, input)
    assertEquals(expected, actual);
  }

  private void testClock(){
    ArrayList<Item> expected = new ArrayList<>(Arrays.asList(new Item(2, 1),
      new Item(4, 1), new Item(5, 0)));
    ArrayList<Integer> input =  new ArrayList<>(Arrays.asList(3, 4, 5, 2, 4));
    ArrayList<Item> actual =  new ArrayList<>();
    //replace with actual clock logic
    //clockLogic(actual, input)
    assertEquals(expected, actual);
  }

  private void testLFU(){
    ArrayList<Item> expected = new ArrayList<>(Arrays.asList(new Item(1, 4), new Item(2, 4),
      new Item(4, 1)));
    ArrayList<Integer> input =  new ArrayList<>(Arrays.asList(1, 2, 3, 1, 1, 2, 2, 3, 4));
    ArrayList<Item> actual =  new ArrayList<>();
    //replace with actual clock logic
    //LFULogic(actual, input)
    assertEquals(expected, actual);
  }

  private class Item{
    public int flag;
    public int value;

    public Item(int valueIn, int flagIn){
      flag = flagIn;
      value = valueIn;
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
}
