/*
 *  This file is part of the Jikes RVM project (http://jikesrvm.org).
 *
 *  This file is licensed to You under the Eclipse Public License (EPL);
 *  You may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *      http://www.opensource.org/licenses/eclipse-1.0.php
 *
 *  See the COPYRIGHT.txt file distributed with this work for information
 *  regarding copyright ownership.
 */
package org.mmtk.plan.concurrent.g1;


import org.mmtk.plan.*;
import org.mmtk.policy.*;
import org.mmtk.utility.Constants;
import org.mmtk.utility.Log;
import org.mmtk.utility.alloc.BlockAllocator;
import org.mmtk.utility.alloc.LinearScan;
import org.mmtk.vm.Lock;
import org.mmtk.vm.Monitor;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.*;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.AddressArray;
import org.vmmagic.unboxed.ObjectReference;
import org.vmmagic.unboxed.Word;

/**
 * This class implements <i>per-collector thread</i> behavior
 * and state for the <i>RegionalCopy</i> plan, which implements a full-heap
 * semi-space collector.<p>
 *
 * Specifically, this class defines <i>RegionalCopy</i> collection behavior
 * (through <code>trace</code> and the <code>collectionPhase</code>
 * method), and collection-time allocation (copying of objects).<p>
 *
 * See {@link G1} for an overview of the semi-space algorithm.
 *
 * @see G1
 * @see G1Mutator
 * @see StopTheWorldCollector
 * @see CollectorContext
 */
@Uninterruptible
public class ConcurrentRemSetRefinement extends CollectorContext {
  @Uninterruptible
  public static class FilledRSBufferQueue {
    public static final int CAPACITY = 512;
    public static final Lock lock = VM.newLock("filled-rs-buffer-queue-lock");
    public static AddressArray array = AddressArray.create(CAPACITY);
    public static int head = 0, tail = 0, size = 0;
    @Inline public static int size() {
      return size;
    }
    @Inline public static boolean isFull() {
      return size == CAPACITY;
    }
    @Inline public static boolean enqueue(Address address) {
      lock.acquire();
      if (size == CAPACITY) {
        lock.release();
        return false;
      }
      array.set(tail, address);
      tail = (tail + 1) % CAPACITY;
      size++;
      lock.release();
      return true;
    }
    @Inline public static Address tryDequeue() {
      lock.acquire();
      if (size == 0) {
        lock.release();
        return Address.zero();
      }
      Address item = array.get(head);
      array.set(head, Address.zero());
      head = (head + 1) % array.length();
      size--;
      lock.release();
      return item;
    }
    @Inline public static void clear() {
      lock.acquire();
      head = 0;
      tail = 0;
      size = 0;
      array = AddressArray.create(CAPACITY);
      lock.release();
    }
  }

  @Uninterruptible
  public static class HotCardsQueue {
    public static final int CAPACITY = 1024;
    public static final Lock lock = VM.newLock("hot-cards-queue-lock");
    public static AddressArray array = AddressArray.create(CAPACITY);
    public static int head = 0, tail = 0, size = 0;
    @Inline public static int size() {
      return size;
    }
    @Inline public static void enqueue(Address address) {
      lock.acquire();
      if (size >= array.length()) {
        AddressArray oldArray = array;
        array = AddressArray.create(oldArray.length() << 1);
        for (int i = 0; i < oldArray.length(); i++)
          array.set(i, oldArray.get(i));
      }
      array.set(tail, address);
      tail = (tail + 1) % array.length();
      size++;
      lock.release();
    }
    @Inline public static Address tryDequeue() {
      lock.acquire();
      if (size == 0) {
        lock.release();
        return Address.zero();
      }
      Address item = array.get(head);
      array.set(head, Address.zero());
      head = (head + 1) % array.length();
      size--;
      lock.release();
      return item;
    }
    @Inline public static void clear() {
      lock.acquire();
      head = 0;
      tail = 0;
      size = 0;
      array = AddressArray.create(CAPACITY);
      lock.release();
    }
  }

  public static final int REMSET_LOG_BUFFER_SIZE = Constants.BYTES_IN_PAGE >>> Constants.LOG_BYTES_IN_ADDRESS;
  public static Monitor monitor;
  public static Lock lock = VM.newLock("RefineLock");

  @Override
  @Interruptible
  public void initCollector(int id) {
    super.initCollector(id);
  }

  @Override
  @Unpreemptible
  public void run() {
    while (true) {
      // Check if the collector is trying to pause refinement threads
      controller.park(this);
      // Do refinement
      refinePartial(1f / 5f);
    }
  }

  /** Runs in mutator threads */
  @Inline
  public static void enqueueFilledRSBuffer(Address buf, boolean triggerConcurrentRefinement) {
    if (triggerConcurrentRefinement) {
      boolean enqueueSuccessful = FilledRSBufferQueue.enqueue(buf);
      if (!enqueueSuccessful || FilledRSBufferQueue.isFull()) {
        controller.triggerCycle();
      }
      if (!enqueueSuccessful) {
        refineSingleBuffer(buf);
      }
    } else if (!Plan.gcInProgress()) {
      if (!FilledRSBufferQueue.enqueue(buf)) {
        refineSingleBuffer(buf);
      }
    } else {
      refineSingleBuffer(buf);
    }
  }

  @Inline
  public static void trigger() {
    monitor.broadcast();
  }

  static TransitiveClosure scanPointers = new TransitiveClosure() {
    @Override @Inline @Uninterruptible public void processEdge(ObjectReference source, Address slot) {
      Address card = Region.Card.of(source);
      ObjectReference ref = slot.loadObjectReference();
      Address value = VM.objectModel.objectStartRef(ref);

      if (source.isNull() || ref.isNull()) return;
      Word tmp = VM.objectModel.objectStartRef(source).toWord().xor(value.toWord());
      tmp = tmp.rshl(Region.LOG_BYTES_IN_REGION);
      if (tmp.isZero()) return;
      if (Space.isMappedAddress(value) && Space.isInSpace(G1.G1, value) && Region.allocated(Region.of(value))) {
        Address foreignBlock = Region.of(ref);
        Region.Card.updateCardMeta(source);
        RemSet.addCard(foreignBlock, card);
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(RemSet.contains(foreignBlock, card));
      }
    }
  };

  static LinearScan cardLinearScan = new LinearScan() {
    @Override @Inline @Uninterruptible public void scan(ObjectReference object) {
      if (object.toAddress().loadWord(Region.Card.OBJECT_END_ADDRESS_OFFSET).isZero()) {
        if (VM.VERIFY_ASSERTIONS) {
          if (!VM.debugging.validRef(object)) {
            Log.writeln();
            Log.write("Space: ");
            Log.writeln(Space.getSpaceForObject(object).getName());
            if (Space.getSpaceForObject(object) instanceof SegregatedFreeListSpace) {
              Log.writeln(BlockAllocator.checkBlockMeta(Region.Card.of(object)) ? " Block Live " : " Block Dead ");
            }
            VM.objectModel.dumpObject(object);
            VM.assertions.fail("");
          }
        }
        VM.scanning.scanObject(scanPointers, object);
      }
    }
  };

  @Inline
  public static void processCard(Address card) {
//    Address region = Region.of(card);
//    if (!Space.isMappedAddress(card) || (Space.isInSpace(G1.G1, card) && !Region.allocated(region))) {
    if (!Space.isMappedAddress(card)) return;
    if (card.LT(VM.AVAILABLE_START) || Space.isInSpace(Plan.VM_SPACE, card)) return;
    if (Space.isInSpace(G1.G1, card) && !Region.allocated(Region.of(card))) return;
//    if (Space.getSpaceForAddress(card) instanceof SegregatedFreeListSpace) {
//      if (!BlockAllocator.checkBlockMeta(card)) return;
//    }
//    if (!Space.isInSpace(Plan.VM_SPACE, card)) {
    long time = VM.statistics.nanoTime();
    Region.Card.linearScan(cardLinearScan, G1.regionSpace, card, false);
    if (Plan.gcInProgress())
      PauseTimePredictor.updateRefinementCardScanningTime(VM.statistics.nanoTime() - time);
//    }
  }

  @Inline
  public static void refineSingleBuffer(Address buffer) {
    for (int i = 0; i < REMSET_LOG_BUFFER_SIZE; i++) {
      Address card = buffer.plus(i << Constants.LOG_BYTES_IN_ADDRESS).loadAddress();
      if (!card.isZero()) {
        if (CardTable.cardIsMarked(card) && CardTable.increaseHotness(card)) {
//          HotCardsQueue.enqueue(card);
        } else {
          if (CardTable.attemptToMarkCard(card, false)) {
            processCard(card);
          }
        }
      }
    }
    Plan.metaDataSpace.release(buffer);
  }

  @Inline
  public static void refineAllDirtyCards() {
    int workers = VM.activePlan.collector().parallelWorkerCount();
    int id = VM.activePlan.collector().getId();
    int totalCards = VM.HEAP_END.diff(VM.HEAP_START).toInt() >>> Region.Card.LOG_BYTES_IN_CARD;
    int cardsToClear = RemSet.ceilDiv(totalCards, workers);

//    for (int i = 0; i < cardsToClear; i++) {
//      int index = id * cardsToClear + i;
//      if (index >= totalCards) break;
//      Address c = VM.HEAP_START.plus(index << Region.Card.LOG_BYTES_IN_CARD);
//      if (CardTable.attemptToMarkCard(c, false)) {
//        processCard(c);
//      }
//    }
    int startIndex = id * cardsToClear, endIndex = id * cardsToClear + cardsToClear;
    for (int i = startIndex; i < endIndex; i++) {
      if (i >= totalCards) break;
      int bufferIndex = i >>> Constants.LOG_BITS_IN_INT;
      if (CardTable.cardTable[bufferIndex] == 0) {
        i = (bufferIndex << Constants.LOG_BITS_IN_INT) + Constants.BITS_IN_INT - 1;
        continue;
      }
      Address c = VM.HEAP_START.plus(i << Region.Card.LOG_BYTES_IN_CARD);
      if (CardTable.attemptToMarkCard(c, false)) {
        processCard(c);
      }
    }

    int rendezvousID = VM.activePlan.collector().rendezvous();
    if (rendezvousID == 0) {
      FilledRSBufferQueue.clear();
      HotCardsQueue.clear();
    }
    if (rendezvousID == 1 || workers == 1) {
      CardTable.clearAllHotness();
    }
    VM.activePlan.collector().rendezvous();
  }

  int lastTriggerCount = 0;
  @Uninterruptible
  static class Controller {
    private final Monitor lock = VM.newHeavyCondLock("lock");
    private final int NUM_WORKERS;
    private volatile boolean pauseRequested = false;
    private volatile int triggerCount = 1, contextsParked = 0;

    public Controller(int numWorkers) {
      this.NUM_WORKERS = numWorkers;
    }

    public void park(ConcurrentRemSetRefinement context) {
      lock.lock();
      context.lastTriggerCount++;
      if (context.lastTriggerCount == triggerCount) {
        contextsParked++;
        lock.broadcast();
        while (context.lastTriggerCount == triggerCount) {
          lock.await();
        }
      }
      lock.unlock();
    }

    public void triggerCycle() {
      lock.lock();
      if (pauseRequested) {
        lock.unlock();
        return;
      }
      triggerCount++;
      contextsParked = 0;
      lock.broadcast();
      lock.unlock();
    }

    public void pause() {
      lock.lock();
      pauseRequested = true;
      lock.unlock();
      waitForCycle();
    }

    /**
     * Wait until the group is idle.
     */
    private void waitForCycle() {
      lock.lock();
      while (contextsParked < NUM_WORKERS) {
        lock.await();
      }
      lock.unlock();
    }

    public void resume() {
      lock.lock();
      pauseRequested = false;
      lock.unlock();
    }
  }

  static Controller controller;

  @Interruptible
  static void initialize(int numWorkers) {
    monitor = VM.newHeavyCondLock("ConcurrentRemSetRefineThreadLock");
    controller = new Controller(numWorkers);
  }

  static void pause() {
    controller.pause();
  }

  static void resume() {
    controller.resume();
  }

  @Inline
  private void refinePartial(float untilRatio) {
    while (true) {
      if (FilledRSBufferQueue.size() <= FilledRSBufferQueue.CAPACITY * untilRatio) {
        return;
      }
      // Dequeue one buffer
      Address buffer = FilledRSBufferQueue.tryDequeue();
      if (!buffer.isZero()) {
        // Process this buffer
        refineSingleBuffer(buffer);
      }
    }
  }

  @Inline
  private static G1 global() {
    return (G1) VM.activePlan.global();
  }
}
