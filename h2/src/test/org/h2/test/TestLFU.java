package org.h2.test;

import org.h2.test.unit.TestCache;
import org.h2.util.CacheClock;
import org.h2.util.CacheObject;

import java.util.*;

public class TestLFU extends TestBase {

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
    testLFU();
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
