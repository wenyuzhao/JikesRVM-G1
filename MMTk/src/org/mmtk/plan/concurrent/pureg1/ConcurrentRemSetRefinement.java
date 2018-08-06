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
package org.mmtk.plan.concurrent.pureg1;


import org.mmtk.plan.*;
import org.mmtk.policy.CardTable;
import org.mmtk.policy.Region;
import org.mmtk.policy.RemSet;
import org.mmtk.policy.Space;
import org.mmtk.utility.Constants;
import org.mmtk.utility.Log;
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
 * See {@link PureG1} for an overview of the semi-space algorithm.
 *
 * @see PureG1
 * @see PureG1Mutator
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

  public static int totalCardsToRefine() {
    return FilledRSBufferQueue.size() << Constants.LOG_BYTES_IN_PAGE + HotCardsQueue.size();
  }
  /*

  @Uninterruptible
  public static class HotCardsQueue {
    public static final Lock lock = VM.newLock("hot-cards-queue-lock");
    public static AddressArray array = AddressArray.create(1024);
    public static int cursor = 0;
    @Inline public static void enqueue(Address address) {
      lock.acquire();
      if (cursor >= array.length()) {
        AddressArray oldArray = array;
        array = AddressArray.create(oldArray.length() << 1);
        for (int i = 0; i < oldArray.length(); i++)
          array.set(i, oldArray.get(i));
      }
      array.set(cursor++, address);
      lock.release();
    }
  }
  */

  public static final int REMSET_LOG_BUFFER_SIZE = Constants.BYTES_IN_PAGE >>> Constants.LOG_BYTES_IN_ADDRESS;
  public static Monitor monitor;
  public static Lock lock = VM.newLock("RefineLock");
  public final int NUM_WORKERS;
  public ConcurrentRemSetRefinement(int numWorkers) {
    NUM_WORKERS = numWorkers;
  }

  @Override
  @Interruptible
  public void initCollector(int id) {
    super.initCollector(id);
    monitor = VM.newHeavyCondLock("ConcurrentRemSetRefineThreadLock");
    controller = new Controller(NUM_WORKERS);
  }

  @Override
  @Unpreemptible
  public void run() {
    while (true) {
      //monitor.await();
      refinePartial(0);
    }
  }

  /** Runs in mutator threads */
  @Inline
  public static void enqueueFilledRSBuffer(Address buf, boolean triggerConcurrentRefinement) {
    if (triggerConcurrentRefinement) {
      while (!FilledRSBufferQueue.enqueue(buf)) {
        Log.writeln("ENQUEUE FAILED");
      }
    } else if (!Plan.gcInProgress()) {
      if (!FilledRSBufferQueue.enqueue(buf)) {
        refineSingleBuffer(buf);
      }
    } else {
      refineSingleBuffer(buf);
    }
//    if (FilledRSBufferQueue.enqueue(buf)) {
//      //if (triggerConcurrentRefinement && FilledRSBufferQueue.size() >= FilledRSBufferQueue.CAPACITY * 4 / 5) trigger();
//      if (triggerConcurrentRefinement) trigger();
//    } else {
//      Log.writeln("! FilledRSBufferQueue is full");
//      refineSingleBuffer(buf);
//    }
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
      /*if (VM.VERIFY_ASSERTIONS) {
        //if (ref.isNull()) VM.assertions._assert(value.isZero());
        VM.assertions._assert(!source.isNull() && !slot.isZero() && !value.isZero());
      }*/
      if (source.isNull() || ref.isNull()) return;
      Word tmp = VM.objectModel.objectStartRef(source).toWord().xor(value.toWord());
      tmp = tmp.rshl(Region.LOG_BYTES_IN_BLOCK);
      //tmp = ref.isNull() ? Word.zero() : tmp;
      if (tmp.isZero()) return;
      if (Space.isMappedAddress(value) && Space.isInSpace(PureG1.MC, value) && Region.allocated(Region.of(value))) {
        Address foreignBlock = Region.of(value);
        //if (Region.relocationRequired(foreignBlock) && Region.usedSize(foreignBlock) == 0) {
        //  return;
        //}

//          if (CardTable.attemptToMarkCard(card, true)) {
//            processCard(card);
//          }
//        } else {
//        if (!Space.isInSpace(PureG1.MC, source) ||
//            (!relocationSetOnly && Region.metaDataOf(foreignBlock, Region.METADATA_GENERATION_OFFSET).loadInt() == 0) ||
//            (relocationSetOnly && (Region.relocationRequired(foreignBlock) || Region.relocationRequired(Region.of(source))))
//        ) {
//          if (Space.isInSpace(PureG1.MC, source) && relocationSetOnly && Region.relocationRequired(Region.of(source))) return;
          RemSet.addCard(foreignBlock, card);
//        }
//        }
      }
    }
  };

  static boolean relocationSetOnly = false;

  static LinearScan cardLinearScan = new LinearScan() {
    @Override @Inline @Uninterruptible public void scan(ObjectReference object) {
      VM.scanning.scanObject(scanPointers, object);
    }
  };

  @Inline
  public static void processCard(Address card) {
    Address region = Region.of(card);
    if (!Space.isMappedAddress(card) || (Space.isInSpace(PureG1.MC, card) && !Region.allocated(region))) {
      return;
    }
//    if (relocationSetOnly && Space.isInSpace(PureG1.MC, card) && (!Region.allocated(region) || Region.relocationRequired(region))) {
//      return;
//    }
    if (!Space.isInSpace(Plan.VM_SPACE, card)) {
//      if (VM.VERIFY_ASSERTIONS) {
//        if (Region.Card.getCardAnchor(card).isZero()) {
//          Log.write("Space ");
//          Log.writeln(Space.getSpaceForAddress(card).getName());
//          if (Space.isInSpace(PureG1.MC, card)) {
//            Log.writeln(Region.relocationRequired(region) ? "relocationRequired=true" : "relocationRequired=false");
//            Log.writeln("Used size: ", Region.usedSize(region));
//          }
//        }
//        VM.assertions._assert(!Region.Card.getCardAnchor(card).isZero());
//      }
      long time = VM.statistics.nanoTime();
      Region.Card.linearScan(cardLinearScan, card, false);
      if (Plan.gcInProgress())
        PauseTimePredictor.updateRefinementCardScanningTime(VM.statistics.nanoTime() - time);
    }
  }

  @Inline
  public static void refineSingleBuffer(Address buffer) {
//    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!buffer.isZero());
    for (int i = 0; i < REMSET_LOG_BUFFER_SIZE; i++) {
      Address card = buffer.plus(i << Constants.LOG_BYTES_IN_ADDRESS).loadAddress();
      if (!card.isZero()) {
        if (CardTable.cardIsMarked(card) && CardTable.increaseHotness(card)) {
          HotCardsQueue.enqueue(card);
        } else {
          if (CardTable.attemptToMarkCard(card, false)) {
            processCard(card);
//            Region.Card.clearCardMeta(card);
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
//    if (concurrent) id -= workers;
    int totalCards = VM.HEAP_END.diff(VM.HEAP_START).toInt() >>> Region.Card.LOG_BYTES_IN_CARD;
    int cardsToClear = RemSet.ceilDiv(totalCards, workers);

    for (int i = 0; i < cardsToClear; i++) {
//      int index = cardsToClear * id + i;
      int index = i * workers + id;
      if (index >= totalCards) break;
      Address c = VM.HEAP_START.plus(index << Region.Card.LOG_BYTES_IN_CARD);
//      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(Region.Card.isAligned(c));
//      if (!CardTable.cardIsMarked(c)) {
        if (CardTable.attemptToMarkCard(c, false)) {
          processCard(c);
//          Region.Card.clearCardMeta(c);
        }
//      };
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

  @Inline
  public static void refineAll() {
    Address buffer;
    while (!(buffer = FilledRSBufferQueue.tryDequeue()).isZero()) {
      refineSingleBuffer(buffer);
    }
    /*
    int workers = VM.activePlan.collector().parallelWorkerCount();
    int id = VM.activePlan.collector().getId();
    int buffersToProcess = RemSet.ceilDiv(FilledRSBufferQueue.size(), workers);

    for (int i = 0; i < buffersToProcess; i++) {
      int cursor = buffersToProcess * id + i;
      if (cursor >= FilledRSBufferQueue.size()) continue;
      cursor = (FilledRSBufferQueue.head + cursor) % FilledRSBufferQueue.CAPACITY;
      Address buffer = FilledRSBufferQueue.array.get(cursor);
      if (buffer.isZero()) continue;
      FilledRSBufferQueue.array.set(cursor, Address.zero());
      refineSingleBuffer(buffer);
    }
    */
  }

  @Inline
  public static void refineHotCards() {
    Address card;
    while (!(card = HotCardsQueue.tryDequeue()).isZero()) {
      if (CardTable.attemptToMarkCard(card, false)) {
        processCard(card);
      }
    }
    /*
    collector.rendezvous();
    int workers = VM.activePlan.collector().parallelWorkerCount();
    int id = VM.activePlan.collector().getId();
    int cardsToProcess = RemSet.ceilDiv(HotCardsQueue.cursor, workers);

    if (VM.VERIFY_ASSERTIONS && id == 0) {
      Log.write("Processing ", HotCardsQueue.cursor);
      Log.writeln(" hot cards ");
    }

    for (int i = 0; i < cardsToProcess; i++) {
      int cursor = cardsToProcess * id + i;
      if (cursor >= HotCardsQueue.cursor) break;
      Address card = HotCardsQueue.array.get(cursor);
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!card.isZero());
      HotCardsQueue.array.set(cursor, Address.zero());
      if (CardTable.attemptToMarkCard(card, false)) {
        processCard(card);
      }
    }
    */
  }

  @Inline
  public static void finishCollectorRefinements() {
    //FilledRSBufferQueue.head = 0;
    //FilledRSBufferQueue.tail = 0;
    //FilledRSBufferQueue.size = 0;
    //HotCardsQueue.cursor = 0;
    CardTable.clearAllHotness();
  }

//  static boolean pause = false;
//  static int paused = 0;
//  static int NUM_WORKERS = 16;

  @Uninterruptible
  static class Controller {
    boolean pause = false;
    int paused = 0;
    Monitor pauseMonitor = VM.newHeavyCondLock("ConcurrentRemSetRefineThreadPausdsadasseLock");
    Monitor pausedMonitor = VM.newHeavyCondLock("ConcurrentRemSetRefineThreadPausedLock");
    final int NUM_WORKERS;

    public Controller(int numWorkers) {
      this.NUM_WORKERS = numWorkers;
    }

    void check() {
      pauseMonitor.lock();
      if (pause) {
        pausedMonitor.lock();
        paused++;
        pausedMonitor.broadcast();
        pausedMonitor.unlock();
        //Log.writeln("Pause refine thread ", paused);
        pauseMonitor.await();
      }
      pauseMonitor.unlock();
    }
    void pause() {
//      Log.writeln("=== PAUSE ===");
      // Request a block
      pauseMonitor.lock();
      paused = 0;
      pause = true;
      //pauseMonitor.broadcast();
      pauseMonitor.unlock();
      // Wait for all thread blocked
//      Log.writeln("Wait for blocked");
      pausedMonitor.lock();
      while (paused < NUM_WORKERS) {
        pausedMonitor.await();
      }
      pausedMonitor.unlock();
//      Log.writeln("All refine threads blocked");
      //VM.assertions.fail("");
    }
    void resume() {
      pauseMonitor.lock();
      pause = false;
      pauseMonitor.broadcast();
      pauseMonitor.unlock();
    }
  }
  static Controller controller;
  static void pause() {
    controller.pause();
  }

  static void resume() {
    controller.resume();
  }

  ///boolean currentThreadPaused = false;

  private static void checkPause() {
//    pauseMonitor.lock();
//    if (pause) {
//      pausedMonitor.lock();
//      paused++;
//      pausedMonitor.broadcast();
//      pausedMonitor.unlock();
//      //Log.writeln("Pause refine thread ", paused);
//      pauseMonitor.await();
//    }
//    pauseMonitor.unlock();
  }

  @Inline
  private void refinePartial(float untilRatio) {
    while (true) {
      // Wait until pause = false
      //Log.writeln("Try Enter ", getId());
      //checkPause();
      controller.check();
      //Log.writeln("Enter ", getId());
//      while (pause) {
//        pausedMonitor.lock();
//        if (!currentThreadPaused) {
//          currentThreadPaused = true;
//          paused++;
//          if (paused >= NUM_WORKERS) {
//            pausedMonitor.broadcast();
//          }
//        }
//        pausedMonitor.unlock();
//        Log.writeln("Await ", getId());
//        pauseMonitor.await();
//      }
      //lock.acquire();
      //Log.writeln("Exit ", getId());
      //currentThreadPaused = false;

      if (FilledRSBufferQueue.size() <= FilledRSBufferQueue.CAPACITY * untilRatio) {
        //lock.release();
        break;
      }
      // Dequeue one buffer
      Address buffer = FilledRSBufferQueue.tryDequeue();
      if (!buffer.isZero()) {
        // Process this buffer
        refineSingleBuffer(buffer);
      }
      //lock.release();
    }
  }

  @Inline
  private static PureG1 global() {
    return (PureG1) VM.activePlan.global();
  }
}
