package org.h2.test;

import org.h2.test.unit.TestCache;
import org.h2.util.CacheClock;
import org.h2.util.CacheObject;

import java.util.*;

public class TestClock extends TestBase {

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
    testClock();
  }

  private void testClock(){
    ArrayList<Item> expected = new ArrayList<>(Arrays.asList(new Item(2, 1),
      new Item(4, 1), new Item(5, 0)));
    ArrayList<Integer> input =  new ArrayList<>(Arrays.asList(3, 4, 5, 2, 4));
    ArrayList<Item> actual =  new ArrayList<>();
    clockLogic(actual, input, 3);
    assertEquals(expected, actual);

    TestCache test = new TestCache();
    CacheClock cache = new CacheClock(test, 4 * 4, false);
    Obj object = new Obj(3);
    Obj object2 = new Obj(4);
    Obj object3 = new Obj(5);
    Obj object4 = new Obj(2);
    Obj object5 = new Obj(4);
    cache.put(object);
    cache.put(object2);
    cache.put(object3);
    cache.put(object4);
    cache.put(object5);

    assertEquals(object3, cache.find(5));
    //way to test order
    CacheObject current = cache.head;
    current = current.cacheNext;
    assertEquals(cache.find(2), current);
    assertEquals(current.flag, 1);
    current = current.cacheNext;
    assertEquals(cache.find(4), current);
    assertEquals(current.flag, 1);
    current = current.cacheNext;
    assertEquals(cache.find(5), current);
    assertEquals(current.flag, 0);
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


}
