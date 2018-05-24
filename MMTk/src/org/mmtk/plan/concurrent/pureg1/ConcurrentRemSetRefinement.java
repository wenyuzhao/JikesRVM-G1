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
  private static final AddressArray[] filledRSBuffers = new AddressArray[16];
  private static int filledRSBuffersCursor = 0;
  private static final Lock filledRSBuffersLock = VM.newLock("filledRSBuffersLock ");

  private static Monitor lock;

  @Override
  @Interruptible
  public void initCollector(int id) {
    super.initCollector(id);
    lock = VM.newHeavyCondLock("ConcurrentRemSetRefineThreadLock");
  }

  @Override
  @Unpreemptible
  public void run() {
    while (true) {
      lock.await();
      refine();
    }
  }

  /** Runs in mutator threads */
  @UninterruptibleNoWarn
  @Inline
  public static void enqueueFilledRSBuffer(AddressArray buf) {
    filledRSBuffersLock.acquire();
    if (filledRSBuffersCursor < filledRSBuffers.length) {
      filledRSBuffers[filledRSBuffersCursor++] = buf;
      if (filledRSBuffersCursor >= filledRSBuffers.length) {
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
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(RemSet.containsCard(foreignBlock, card));
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
      //Log.writeln("processCard Scan ", object);
      //if (!object.isNull() && isLive(object))
      /*Space space = Space.getSpaceForObject(object);
      if (Plan.gcInProgress()) {

      } else if (PureG1.concurrentMarkingInProgress) {

      } else {
        if (!space.isLive(object)) {
          VM.objectModel.writeAvailableBitsWord(object, Word.zero());
        }
      }*/
      VM.scanning.scanObject(scanPointers, object);
    }
  };

  public static void processCard(Address card) {
    /*Log.write("Process card ", card);
    Log.write(", range ", MarkBlock.Card.getCardAnchor(card));
    Log.write(" ..< ", MarkBlock.Card.getCardLimit(card));
    Log.write(", offsets ", MarkBlock.Card.getByte(MarkBlock.Card.anchors, MarkBlock.Card.hash(card)));
    Log.write(" ..< ", MarkBlock.Card.getByte(MarkBlock.Card.limits, MarkBlock.Card.hash(card)));
    Log.write(" in space: ");
    Log.writeln(Space.getSpaceForAddress(card).getName());*/

    if (!Space.isInSpace(Plan.VM_SPACE, card)) {
      if (VM.VERIFY_ASSERTIONS) {
        VM.assertions._assert(!Region.Card.getCardAnchor(card).isZero());
      }
      Region.Card.linearScan(cardLinearScan, card);
    }
  }

  public static void refineSingleBuffer(AddressArray buffer) {
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(buffer != null);
    for (int i = 0; i < buffer.length(); i++) {
      Address card = buffer.get(i);
      if (!card.isZero()) {
        if (CardTable.attemptToMarkCard(card, false)) {
          //Log.writeln("Unmark card ", card);
          processCard(card);
        }
      }
    }
  }

  public static final Lock refineLock = VM.newLock("refineLock");

  @UninterruptibleNoWarn
  public static void refine() {
    //refineLock.acquire();
    if (VM.VERIFY_ASSERTIONS) Log.writeln("CONCURRENT REMSET REFINEMENT");
    refineAll();
  }

  @UninterruptibleNoWarn
  public static void refineAll() {
    refineLock.acquire();
    for (int i = 0; i < filledRSBuffers.length; i++) {
      if (filledRSBuffers[i] != null) {
        refineSingleBuffer(filledRSBuffers[i]);
      }
      filledRSBuffers[i] = null;
    }
    filledRSBuffersLock.acquire();
    filledRSBuffersCursor = 0;
    filledRSBuffersLock.release();
    refineLock.release();
  }



  @Inline
  private static PureG1 global() {
    return (PureG1) VM.activePlan.global();
  }
}
