/*
 * Copyright 2004-2022 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import org.h2.engine.Constants;
import org.h2.engine.SysProperties;
import org.h2.message.DbException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

/**
 * A cache implementation based on the last frequently used (LFU) algorithm.
 */

public class CacheLFU implements Cache {

// TODO: Need to think about what we do when we multiple 1000...0000 * 2?
    
// TODO: Find places where we will need to double the flag

    static final String TYPE_NAME = "LFU";

    private final CacheWriter writer;

    /**
     * Use First-In-First-Out (don't move recently used items to the front of
     * the queue).
     */
    private final boolean fifo;

    // TODO: Do we need to use an ArrayList??
    private final CacheObject head = new CacheHead();
    private final int mask;
    private CacheObject[] values;
    private int recordCount;

    /**
     * The number of cache buckets.
     */
    private final int len;

    /**
     * The maximum memory, in words (4 bytes each).
     */
    private long maxMemory;

    /**
     * The current memory used in this cache, in words (4 bytes each).
     */
    private long memory;

    /**
     * Tracks the hand location for clock algorithm
     */

// private CacheObject hand = head;

    // TODO: Update this class (flag etc)!
    public CacheLFU(CacheWriter writer, int maxMemoryKb, boolean fifo) {
        this.writer = writer;
        this.fifo = fifo;
        this.setMaxMemory(maxMemoryKb);
        try {
            // Since setMaxMemory() ensures that maxMemory is >=0,
            // we don't have to worry about an underflow.
            long tmpLen = maxMemory / 64;
            if (tmpLen > Integer.MAX_VALUE) {
                throw new IllegalArgumentException();
            }
            this.len = MathUtils.nextPowerOf2((int) tmpLen);
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("This much cache memory is not supported: " + maxMemoryKb + "kb", e);
        }
        this.mask = len - 1;
        clear();
    }

    /**
     * Create a cache of the given type and size.
     *
     * @param writer the cache writer
     * @param cacheType the cache type
     * @param cacheSize the size
     * @return the cache object
     */
    public static Cache getCache(CacheWriter writer, String cacheType,
            int cacheSize) {
        Map<Integer, CacheObject> secondLevel = null;
        if (cacheType.startsWith("SOFT_")) {
            secondLevel = new SoftValuesHashMap<>();
            cacheType = cacheType.substring("SOFT_".length());
        }
        Cache cache;
        if (CacheClock.TYPE_NAME.equals(cacheType)) {
            cache = new CacheClock(writer, cacheSize, false);
        } else if (CacheTQ.TYPE_NAME.equals(cacheType)) {
            cache = new CacheTQ(writer, cacheSize);
        } else {
            throw DbException.getInvalidValueException("CACHE_TYPE", cacheType);
        }
        if (secondLevel != null) {
            cache = new CacheSecondLevel(cache, secondLevel);
        }
        return cache;
    }

    @Override
    public void clear() {
        head.cacheNext = head.cachePrevious = head;
        // first set to null - avoiding out of memory
        values = null;
        values = new CacheObject[len];
        recordCount = 0;
        memory = len * (long)Constants.MEMORY_POINTER;
    }

    // TODO: Here is a place to double the flag
    @Override
    public void put(CacheObject rec) {
        if (SysProperties.CHECK) {
            int pos = rec.getPos();
            CacheObject old = find(pos);
            if (old != null) {
                old.flag = 1; //maybe
                return;
            }
        }
        int index = rec.getPos() & mask;
        rec.cacheChained = values[index];
        values[index] = rec;

        removeOldIfRequired(rec.getMemory());
        //insertAtHand(rec);

    }

    // check if CacheObject record is in memory, if it is not, put it in. If it is, throw an error?
    @Override
    public CacheObject update(int key, CacheObject rec) {
        CacheObject old = find(key);
        if (old == null) {
            put(rec);
        } else {
            if (old != rec) {
                throw DbException.getInternalError("old!=record pos:" + key + " old:" + old + " new:" + rec);
            }

            // if (!fifo) {
            //     removeFromLinkedList(rec);
            //     addToFront(rec);
            // }

        }
        return old;
    }

    private void removeOldIfRequired(long objectMemSize) {
        // a small method, to allow inlining
        if (memory >= maxMemory) {
            removeOld(objectMemSize);
        }
    }

    // TODO: Need to read through and update this function for LFU
    private void removeOld(long objectMemSize) {
        int i = 0;
        ArrayList<CacheObject> changed = new ArrayList<>();
        long mem = memory;
        int rc = recordCount;
        boolean flushed = false;
        CacheObject next = null;
        int memSize = 0;
        int count = 0;


        while(maxMemory - objectMemSize < mem){
            if(count == rc){
                writer.getTrace()
                  .info("cannot remove records, cache size too small? records:" +
                    recordCount + " memory:" + memory);
                break;
            }
//            if(hand.flag == 0 && hand.canRemove()){
//                memSize = hand.getMemory();
//                next = hand.cacheNext;
//                changed.add(hand);
//                rc -= 1;
//                mem -= memSize;
//                count -= 1;
//            }
//            else {
//                hand.flag = 0;
//            }
//
//            hand = next;
//            count += 1;

        }

        long max = maxMemory;
        int size = changed.size();
        try {
            // temporary disable size checking,
            // to avoid stack overflow
            maxMemory = Long.MAX_VALUE;
            for (i = 0; i < size; i++) {
                CacheObject rec = changed.get(i);
                writer.writeBack(rec);
            }
        } finally {
            maxMemory = max;
        }

        while (!changed.isEmpty()){
            CacheObject rec = changed.get(i);
            remove(rec.getPos()); // Note: getPos() gets key not position
            if (rec.cacheNext != null) {
                throw DbException.getInternalError();
            }
        }
    }

    // TODO: We insert differently than clock, use addToFront??

    // private void insertAtHand(CacheObject rec){
    //     rec.cachePrevious = hand.cachePrevious;
    //     rec.cachePrevious.cacheNext = rec;
    //     rec.cacheNext = hand;
    //     hand.cachePrevious = rec;
    //     recordCount++;
    //     memory += rec.getMemory();
    // }

    // TODO: Consider adding to back instead (right before head)
    private void addToFront(CacheObject rec) {
        if (rec == head) {
            throw DbException.getInternalError("try to move head");
        }
        rec.cacheNext = head;
        rec.cachePrevious = head.cachePrevious;
        rec.cachePrevious.cacheNext = rec;
        head.cachePrevious = rec;
    }

    // TODO: Are we even using a linked list?
    private void removeFromLinkedList(CacheObject rec) {
        if (rec == head) {
            throw DbException.getInternalError("try to remove head");
        }
        rec.cachePrevious.cacheNext = rec.cacheNext;
        rec.cacheNext.cachePrevious = rec.cachePrevious;
        rec.cacheNext = null;
        rec.cachePrevious = null;
    }

    // TODO: Double check, but should be almost the same, maybe change remove helper function?
    @Override
    public boolean remove(int key) {
        int index = key & mask;
        CacheObject rec = values[index];
        if (rec == null) {
            return false;
        }
        if (rec.getPos() == key) {
            values[index] = rec.cacheChained;
        } else {
            CacheObject last;
            do {
                last = rec;
                rec = rec.cacheChained;
                if (rec == null) {
                    return false;
                }
            } while (rec.getPos() != key);
            last.cacheChained = rec.cacheChained;
        }
        recordCount--;
        memory -= rec.getMemory();
        removeFromLinkedList(rec);
        if (SysProperties.CHECK) {
            rec.cacheChained = null;
            CacheObject o = find(key);
            if (o != null) {
                throw DbException.getInternalError("not removed: " + o);
            }
        }
        return true;
    }

    // TODO: Change if not a linked list
    @Override
    public CacheObject find(int key) {
        CacheObject rec = values[key & mask];
        while (rec != null && rec.getPos() != key) {
            rec = rec.cacheChained;
        }
        return rec;
    }

    // TODO: Update the rec.flag line, I don't think we need it.
    @Override
    public CacheObject get(int key) {
        CacheObject rec = find(key);
        if (rec != null) {
            rec.flag = 1;
        }
        return rec;
    }

    @Override
    public ArrayList<CacheObject> getAllChanged() {
        // if(Database.CHECK) {
        // testConsistency();
        // }
        ArrayList<CacheObject> list = new ArrayList<>();
        CacheObject rec = head.cacheNext;
        while (rec != head) {
            if (rec.isChanged()) {
                list.add(rec);
            }
            rec = rec.cacheNext;
        }
        return list;
    }

    @Override
    public void setMaxMemory(int maxKb) {
        long newSize = maxKb * 1024L / 4;
        long temp = maxMemory - newSize;
        maxMemory = newSize < 0 ? 0 : newSize;
        // can not resize, otherwise existing records are lost
        // resize(maxSize);
        removeOldIfRequired(temp);
    }

    @Override
    public int getMaxMemory() {
        return (int) (maxMemory * 4L / 1024);
    }

    @Override
    public int getMemory() {
        // CacheObject rec = head.cacheNext;
        // while (rec != head) {
        // System.out.println(rec.getMemory() + " " +
        // MemoryFootprint.getObjectSize(rec) + " " + rec);
        // rec = rec.cacheNext;
        // }
        return (int) (memory * 4L / 1024);
    }

}
