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
package org.mmtk.plan.markcopy.remset;


import org.mmtk.plan.CollectorContext;
import org.mmtk.plan.StopTheWorldCollector;
import org.mmtk.plan.TransitiveClosure;
import org.mmtk.policy.MarkBlock;
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

/**
 * This class implements <i>per-collector thread</i> behavior
 * and state for the <i>RegionalCopy</i> plan, which implements a full-heap
 * semi-space collector.<p>
 *
 * Specifically, this class defines <i>RegionalCopy</i> collection behavior
 * (through <code>trace</code> and the <code>collectionPhase</code>
 * method), and collection-time allocation (copying of objects).<p>
 *
 * See {@link MarkCopy} for an overview of the semi-space algorithm.
 *
 * @see MarkCopy
 * @see MarkCopyMutator
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
  public static void enqueueFilledRSBuffer(AddressArray buf) {
    Log.write("enqueueFilledRSBuffer {");
    Log.flush();

    /*filledRSBuffersLock.acquire();
    if (filledRSBuffersCursor < filledRSBuffers.length) {
      filledRSBuffers[filledRSBuffersCursor++] = buf;
      if (filledRSBuffersCursor >= filledRSBuffers.length) {
        ConcurrentRemSetRefinement.trigger();
      }
      filledRSBuffersLock.release();
    } else {
      filledRSBuffersLock.release();
      refineSingleBuffer(buf);
    }*/
    //refineSingleBuffer(buf);

    Log.writeln("}");
    Log.flush();
  }

  public static void trigger() {
    lock.broadcast();
  }

  static TransitiveClosure scanPointers = new TransitiveClosure() {
    @Override @Uninterruptible public void processEdge(ObjectReference source, Address slot) {
      Address card = MarkBlock.Card.of(VM.objectModel.objectStartRef(source));
      ObjectReference object = global().loadObjectReference(slot);
      Address ptr = slot.loadAddress();
      if (!object.isNull() && Space.isInSpace(MarkCopy.MC, object) && MarkBlock.of(ptr).NE(MarkBlock.of(source.toAddress()))) { // foreign pointer to MC space
        Address foreignBlock = MarkBlock.of(ptr);
        //RemSet.addCard(foreignBlock, card);
      }
    }
  };

  static LinearScan cardLinearScan = new LinearScan() {
    @Override @Uninterruptible public void scan(ObjectReference object) {
      VM.scanning.scanObject(scanPointers, object);
    }
  };

  public static void processCard(Address card) {
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!MarkBlock.Card.getFirstObjectAddressInCard(card).isZero());
    Log.writeln("Linear scan card ", card);
    MarkBlock.Card.linearScan(cardLinearScan, card);
  }

  public static void refineSingleBuffer(AddressArray buffer) {
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(buffer != null);
    for (int i = 0; i < buffer.length(); i++) {
      processCard(buffer.get(i));
    }
  }

  public static final Lock refineLock = VM.newLock("refineLock");

  @UninterruptibleNoWarn
  public static void refine() {
    refineLock.acquire();
    Log.writeln("Concurrent Refine");
    for (int i = 0; i < filledRSBuffersCursor; i++) {
      refineSingleBuffer(filledRSBuffers[i]);
      filledRSBuffers[i] = null;
    }
    filledRSBuffersLock.acquire();
    filledRSBuffersCursor = 0;
    filledRSBuffersLock.release();
    //while (global().filledRSBufferSize() > MarkCopy.CONCURRENT_REFINEMENT_THRESHOLD) {
      //AddressArray buffer = global().dequeueFilledRSBuffer();
      //refineSingleBuffer(buffer);
    //}

    refineLock.release();
  }

  @Inline
  private static MarkCopy global() {
    return (MarkCopy) VM.activePlan.global();
  }
}
