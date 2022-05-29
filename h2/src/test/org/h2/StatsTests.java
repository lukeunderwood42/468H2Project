package org.h2;

import org.h2.test.TestBase;
import org.h2.test.unit.TestCache;
import org.h2.util.Cache;
import org.h2.util.CacheClock;
import org.h2.util.CacheObject;

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

  }

  public void testClockLoad(Obj[] inputs, int size){
    TestCache test = new TestCache();
    CacheClock cache = new CacheClock(test, size * 4, false);
  }

  public void testAllWithInputs(Obj[] inputs, int size){

  }






}
