package org.h2.test;

import org.h2.test.unit.TestCache;
import org.h2.util.CacheLFU;
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


    TestCache test = new TestCache();
    CacheLFU cache = new CacheLFU(test, 15 * 4);
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
    assertEquals(null, cache.find(1));
    cache.put(object);
    cache.put(object2);
    cache.put(object3);
    cache.put(object4);
    cache.put(object5);
   cache.put(object6);
   cache.put(object7);
   cache.put(object8);
   cache.put(object9);
   cache.put(object10);

    assertNotNull(cache.find(1));
    assertNotNull(cache.find(2));
    assertNotNull(cache.find(3));
    assertNotNull(cache.find(4));
    assertNotNull(cache.find(5));
    assertNotNull(cache.find(6));
    assertNotNull(cache.find(7));
    assertNotNull(cache.find(8));
    assertNotNull(cache.find(9));
    assertNotNull(cache.find(10));



    TestCache test2 = new TestCache();
    CacheLFU cache2 = new CacheLFU(test2, 4 * 4);
    Obj objectB = new Obj(1);
    Obj objectB2 = new Obj(3);
    Obj objectB3 = new Obj(5);
    Obj objectB4 = new Obj(2);

    assertEquals(null, cache2.find(1));
    cache2.put(objectB);
    cache2.put(objectB2);
    cache2.put(objectB3);
    cache2.get(1);
    cache2.get(3);
    cache2.put(objectB4);
    assertNotNull(cache2.find(1));
    assertNotNull(cache2.find(3));
    assertNotNull(cache2.find(2));
    assertNull(cache2.find(5));


    TestCache test3 = new TestCache();
    CacheLFU cache3 = new CacheLFU(test3, 3 * 4);
    Obj objectC = new Obj(1);
    Obj objectC2 = new Obj(3);
    Obj objectC3 = new Obj(5);

    assertEquals(null, cache3.find(1));
    cache3.put(objectC);
    cache3.put(objectC2);
    //cache3.put(objectC3);
    cache3.get(1);
    cache3.get(1);
    cache3.get(1);
    cache3.get(1);
    cache3.get(3);
    cache3.get(3);
    cache3.get(3);
    cache3.put(objectC3);
    assertNotNull(cache3.find(1));
    assertNotNull(cache3.find(5));
    assertNull(cache3.find(3));

    TestCache test4 = new TestCache();
    CacheLFU cache4 = new CacheLFU(test4, 4 * 4);
    Obj objectD = new Obj(1);
    Obj objectD2 = new Obj(3);
    Obj objectD3 = new Obj(5);
    Obj objectD4 = new Obj(6);

    assertEquals(null, cache4.find(1));
    cache4.put(objectD);
    cache4.put(objectD2);
    cache4.put(objectD3);
    cache4.put(objectD4);
    //cache3.put(objectC3);
    assertNotNull(cache4.find(3));
    assertNotNull(cache4.find(5));
    assertNotNull(cache4.find(6));
    assertNull(cache4.find(1));

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
