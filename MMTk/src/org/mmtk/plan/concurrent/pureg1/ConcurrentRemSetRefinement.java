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
import org.mmtk.utility.deque.SharedDeque;
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
  public static final int REMSET_LOG_BUFFER_SIZE = Constants.BYTES_IN_PAGE >> Constants.LOG_BYTES_IN_ADDRESS;
  //public static final SharedDeque cardBufPool = new SharedDeque("modBufs", Plan.metaDataSpace, 1);
  //private static final AddressArray[] filledRSBuffers = new AddressArray[16];
  private static final AddressArray filledRSBuffers = AddressArray.create(16);
  private static int filledRSBuffersCursor = 0;
  private static final Lock filledRSBuffersLock = VM.newLock("filledRSBuffersLock ");
  private static AddressArray hotCardsBuffer = AddressArray.create(1024);
  private static int hotCardsBufferCursor = 0;

  private static Monitor lock;
  //private static Monitor refineLock;

  @Override
  @Interruptible
  public void initCollector(int id) {
    super.initCollector(id);
    lock = VM.newHeavyCondLock("ConcurrentRemSetRefineThreadLock");
    //refineLock = VM.newHeavyCondLock("ConcurrentRemSetRefineThreadLock-refineLock");
  }
  public static boolean hasWork() {
    return filledRSBuffersCursor != 0 || hotCardsBufferCursor != 0;
  }
  public static boolean inProgress() {
    return inProgress;
  }
  private static boolean inProgress = false;

  @Override
  @Unpreemptible
  public void run() {
    while (true) {
      lock.await();
      refineLock.acquire();
      inProgress = true;
      refineLock.release();
      refine();
      refineLock.acquire();
      inProgress = false;
      refineLock.release();
    }
  }

  /** Runs in mutator threads */
  @Inline
  @Uninterruptible
  public static void enqueueFilledRSBuffer(Address buf, boolean triggerConcurrentRefinement) {
    filledRSBuffersLock.acquire();
    if (triggerConcurrentRefinement && filledRSBuffersCursor < filledRSBuffers.length()) {
      filledRSBuffers.set(filledRSBuffersCursor++, buf);
      //ObjectReference.fromObject(filledRSBuffers).toAddress().plus()
      if (filledRSBuffersCursor >= filledRSBuffers.length()) {
        ConcurrentRemSetRefinement.trigger();
      }
      filledRSBuffersLock.release();
    } else {
      filledRSBuffersLock.release();
      refineSingleBuffer(buf);
    }
  }

  public static void trigger() {
    lock.broadcast();
  }

  static TransitiveClosure scanPointers = new TransitiveClosure() {
    @Override @Uninterruptible public void processEdge(ObjectReference source, Address slot) {
      Address card = Region.Card.of(source);
      ObjectReference ref = slot.loadObjectReference();
      Address value = VM.objectModel.objectStartRef(ref);
      /*Log.write("processCard scanPointers ", source);
      Log.write(".", slot);
      Log.writeln(": ", VM.activePlan.global().loadObjectReference(slot));*/
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!source.isNull() && !slot.isZero() && !value.isZero());
      Word tmp = slot.toWord().xor(value.toWord());
      tmp = tmp.rshl(Region.LOG_BYTES_IN_BLOCK);
      tmp = value.isZero() ? Word.zero() : tmp;
      if (tmp.isZero()) return;
      if (Space.isInSpace(PureG1.MC, value)) {
        Address foreignBlock = Region.of(value);
        //Log.write("Add card ", card);
        //Log.writeln(" to remset of block ", foreignBlock);
        RemSet.addCard(foreignBlock, card);
        //if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(RemSet.containsCard(foreignBlock, card));
      }
      /*
      Address ptr = slot.loadAddress();
      if (!ptr.isZero() && Space.isInSpace(PureG1.MC, ptr) && MarkBlock.of(ptr).NE(MarkBlock.of(source.toAddress()))) { // cross block pointer
        Address foreignBlock = MarkBlock.of(ptr);
        Log.write("Add card ", card);
        Log.writeln(" to remset of block ", foreignBlock);
        RemSet.addCard(foreignBlock, card);
      }
      */
    }
  };

  static LinearScan cardLinearScan = new LinearScan() {
    @Override @Uninterruptible public void scan(ObjectReference object) {
      VM.scanning.scanObject(scanPointers, object);
    }
  };

  @Uninterruptible
  public static void processCard(Address card) {
    if (!Space.isInSpace(Plan.VM_SPACE, card)) {
      if (VM.VERIFY_ASSERTIONS) {
        VM.assertions._assert(!Region.Card.getCardAnchor(card).isZero());
      }
      Region.Card.linearScan(cardLinearScan, card, false);
    }
  }

  @Uninterruptible
  public static void refineSingleBuffer(Address buffer) {
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(buffer != null);
    for (int i = 0; i < REMSET_LOG_BUFFER_SIZE; i++) {
      Address card = buffer.plus(i << Constants.LOG_BYTES_IN_ADDRESS).loadAddress();
      if (!card.isZero()) {
        if (CardTable.cardIsMarked(card) && CardTable.increaseHotness(card)) {
          if (hotCardsBufferCursor >= hotCardsBuffer.length()) {
            Log.writeln("Expand hot cards buffer size");
            AddressArray oldBuffer = hotCardsBuffer;
            hotCardsBuffer = AddressArray.create(oldBuffer.length() << 1);
            for (int j = 0; j < oldBuffer.length(); j++)
              hotCardsBuffer.set(j, oldBuffer.get(i));
          }
          hotCardsBuffer.set(hotCardsBufferCursor++, card);
        } else {
          if (CardTable.attemptToMarkCard(card, false)) {
            processCard(card);
          }
        }
      }
    }
  }

  public static final Lock refineLock = VM.newLock("refineLock");

  @Uninterruptible
  public static void refine() {
    //refineLock.acquire();
    //if (VM.VERIFY_ASSERTIONS) Log.writeln("CONCURRENT REMSET REFINEMENT");
    refinePartial(4/5);
  }

  @Uninterruptible
  public static void refineAll() {
    refinePartial(1);
    if (VM.VERIFY_ASSERTIONS) {
      VM.assertions._assert(filledRSBuffersCursor == 0);
    }
    /*
    // Process hot cards
    Log.write("Processing ", hotCardsBufferCursor);
    Log.writeln(" hot cards ", hotCardsBuffer.length());
    refineLock.acquire();
    for (int i = 0; i < hotCardsBufferCursor; i++) {
      Address card = hotCardsBuffer.get(i);
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!card.isZero());
      if (CardTable.attemptToMarkCard(card, false)) {
        processCard(card);
      }
      hotCardsBuffer.set(i, Address.zero());
    }
    hotCardsBufferCursor = 0;
    CardTable.clearAllHotness();
    refineLock.release();
    */
  }

  @Uninterruptible
  public static void refineHotCards() {
    //refineLock.acquire();
    int workers = VM.activePlan.collector().parallelWorkerCount();
    int id = VM.activePlan.collector().getId();
    int cardsToProcess = RemSet.ceilDiv(hotCardsBufferCursor, workers);

    if (VM.VERIFY_ASSERTIONS && id == 0) {
      // Process hot cards
      Log.write("Processing ", hotCardsBufferCursor);
      Log.writeln(" hot cards ", hotCardsBuffer.length());
    }

    for (int i = 0; i < cardsToProcess; i++) {
      int cursor = cardsToProcess * id + i;
      if (cursor >= hotCardsBufferCursor) break;
      Address card = hotCardsBuffer.get(cursor);
      if (VM.VERIFY_ASSERTIONS) {
        if (card.isZero()) {
          Log.writeln("Null HotCard at index ", cursor);
        }
        VM.assertions._assert(!card.isZero());
      }
      if (CardTable.attemptToMarkCard(card, false)) {
        processCard(card);
      }
      hotCardsBuffer.set(cursor, Address.zero());
    }
    //refineLock.release();
  }

  @Uninterruptible
  public static void finishRefineHotCards() {
    refineLock.acquire();
    hotCardsBufferCursor = 0;
    CardTable.clearAllHotness();
    refineLock.release();
    //CardTable.assertAllCardsAreNotMarked();
  }

  @Uninterruptible
  private static void refinePartial(float ratio) {
    refineLock.acquire();
    int start = (int) (filledRSBuffers.length() - (filledRSBuffers.length() * ratio));
    for (int i = filledRSBuffers.length() - 1; i >= start; i--) {
      Address buf = filledRSBuffers.get(i);
      if (!buf.isZero()) {
        filledRSBuffers.set(i, Address.zero());
        refineSingleBuffer(buf);
      }
    }
    filledRSBuffersLock.acquire();
    filledRSBuffersCursor = start;
    filledRSBuffersLock.release();
    refineLock.release();
  }

  @Inline
  private static PureG1 global() {
    return (PureG1) VM.activePlan.global();
  }
}
