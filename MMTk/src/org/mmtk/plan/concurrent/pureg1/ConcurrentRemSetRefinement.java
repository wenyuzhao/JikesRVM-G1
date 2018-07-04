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


import org.mmtk.plan.CollectorContext;
import org.mmtk.plan.Plan;
import org.mmtk.plan.StopTheWorldCollector;
import org.mmtk.plan.TransitiveClosure;
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
    public static final AddressArray array = AddressArray.create(CAPACITY);
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
  }

  @Uninterruptible
  public static class HotCardsQueue {
    public static final Lock lock = VM.newLock("hot-cards-queue-lock");
    public static AddressArray array = AddressArray.create(1024);
    public static int head = 0, tail = 0, size = 0;
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

  public static final int REMSET_LOG_BUFFER_SIZE = Constants.BYTES_IN_PAGE >> Constants.LOG_BYTES_IN_ADDRESS;
  public static Monitor monitor;
  public static Lock lock = VM.newLock("RefineLock");

  @Override
  @Interruptible
  public void initCollector(int id) {
    super.initCollector(id);
    monitor = VM.newHeavyCondLock("ConcurrentRemSetRefineThreadLock");
  }

  @Override
  @Unpreemptible
  public void run() {
    while (true) {
      monitor.await();
      refinePartial(1/5);
    }
  }

  /** Runs in mutator threads */
  @Inline
  public static void enqueueFilledRSBuffer(Address buf, boolean triggerConcurrentRefinement) {
    if (FilledRSBufferQueue.enqueue(buf)) {
      //if (triggerConcurrentRefinement && FilledRSBufferQueue.size() >= FilledRSBufferQueue.CAPACITY * 4 / 5) trigger();
      if (triggerConcurrentRefinement) trigger();
    } else {
      Log.writeln("! FilledRSBufferQueue is full");
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
      /*if (VM.VERIFY_ASSERTIONS) {
        //if (ref.isNull()) VM.assertions._assert(value.isZero());
        VM.assertions._assert(!source.isNull() && !slot.isZero() && !value.isZero());
      }*/
      if (source.isNull() || ref.isNull()) return;
      Word tmp = VM.objectModel.objectStartRef(source).toWord().xor(value.toWord());
      tmp = tmp.rshl(Region.LOG_BYTES_IN_BLOCK);
      //tmp = ref.isNull() ? Word.zero() : tmp;
      if (tmp.isZero()) return;
      if (Space.isInSpace(PureG1.MC, value)) {
        Address foreignBlock = Region.of(value);
        RemSet.addCard(foreignBlock, card);
      }
    }
  };

  static LinearScan cardLinearScan = new LinearScan() {
    @Override @Inline @Uninterruptible public void scan(ObjectReference object) {
      VM.scanning.scanObject(scanPointers, object);
    }
  };

  @Inline
  public static void processCard(Address card) {
    if (!Space.isInSpace(Plan.VM_SPACE, card)) {
      if (VM.VERIFY_ASSERTIONS) {
        VM.assertions._assert(!Region.Card.getCardAnchor(card).isZero());
      }
      Region.Card.linearScan(cardLinearScan, card, false);
    }
  }

  @Inline
  public static void refineSingleBuffer(Address buffer) {
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!buffer.isZero());
    for (int i = 0; i < REMSET_LOG_BUFFER_SIZE; i++) {
      Address card = buffer.plus(i << Constants.LOG_BYTES_IN_ADDRESS).loadAddress();
      if (!card.isZero()) {
        if (CardTable.cardIsMarked(card) && CardTable.increaseHotness(card)) {
          HotCardsQueue.enqueue(card);
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

  @Inline
  private static void refinePartial(float untilRatio) {
    while (!Plan.gcInProgress()) {
      lock.acquire();
      if (FilledRSBufferQueue.size() <= FilledRSBufferQueue.CAPACITY * untilRatio) {
        lock.release();
        break;
      }
      // Dequeue one buffer
      Address buffer = FilledRSBufferQueue.tryDequeue();
      if (!buffer.isZero()) {
        // Process this buffer
        refineSingleBuffer(buffer);
      }
      lock.release();
    }
  }

  @Inline
  private static PureG1 global() {
    return (PureG1) VM.activePlan.global();
  }
}
