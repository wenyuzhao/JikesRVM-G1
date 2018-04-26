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
import org.mmtk.utility.Log;
import org.mmtk.utility.alloc.LinearScan;
import org.mmtk.vm.Monitor;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Interruptible;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.pragma.Unpreemptible;
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

  public static void trigger() {
    lock.broadcast();
  }

  TransitiveClosure scanPointers = new TransitiveClosure() {
    @Override @Uninterruptible public void processEdge(ObjectReference source, Address slot) {
      Address card = MarkBlock.Card.of(VM.objectModel.objectStartRef(source));
      Address ptr = slot.loadAddress();
      if (MarkBlock.Card.of(ptr).NE(card)) {
        Address foreignBlock = MarkBlock.of(ptr);
        RemSet.addCard(foreignBlock, card);
      }
    }
  };

  LinearScan cardLinearScan = new LinearScan() {
    @Override @Uninterruptible public void scan(ObjectReference object) {
      VM.scanning.scanObject(scanPointers, object);
    }
  };

  public void processCard(Address card) {
    MarkBlock.Card.linearScan(cardLinearScan, card);
  }

  public void refine() {
    Log.writeln("Concurrent Refine");
    MarkCopy global = (MarkCopy) VM.activePlan.global();
    while (global.filledRSBufferSize() > MarkCopy.CONCURRENT_REFINEMENT_THRESHOLD) {
      AddressArray buffer = ((MarkCopy) VM.activePlan.global()).dequeueFilledRSBuffer();
      for (int i = 0; i < buffer.length(); i++) {
        processCard(buffer.get(i));
      }
    }
  }
}
