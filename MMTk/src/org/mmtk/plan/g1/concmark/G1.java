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
package org.mmtk.plan.g1.concmark;

import org.mmtk.plan.Phase;
import org.mmtk.utility.deque.SharedDeque;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Interruptible;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.ObjectReference;
import org.vmmagic.unboxed.Word;

/**
 * This class implements a simple region-based collector.
 */
@Uninterruptible
public class G1 extends org.mmtk.plan.g1.baseline.G1 {
  public static final short FLUSH_MUTATOR               = Phase.createSimple("flush-mutator", null);
  public static final short SET_BARRIER_ACTIVE          = Phase.createSimple("set-barrier", null);
  public static final short FLUSH_COLLECTOR             = Phase.createSimple("flush-collector", null);
  public static final short CLEAR_BARRIER_ACTIVE        = Phase.createSimple("clear-barrier", null);
  public static final short FINAL_MARK                  = Phase.createSimple("final-mark", null);

  protected static final short preemptConcurrentClosure = Phase.createComplex("preeempt-concurrent-trace", null,
      Phase.scheduleMutator  (FLUSH_MUTATOR),
      Phase.scheduleCollector(CLOSURE)
  );

  public static final short CONCURRENT_CLOSURE = Phase.createConcurrent("concurrent-closure",
      Phase.scheduleComplex(preemptConcurrentClosure)
  );

  protected static final short concurrentClosure = Phase.createComplex("concurrent-mark", null,
      Phase.scheduleGlobal    (SET_BARRIER_ACTIVE),
      Phase.scheduleMutator   (SET_BARRIER_ACTIVE),
      Phase.scheduleCollector (FLUSH_COLLECTOR),
      Phase.scheduleConcurrent(CONCURRENT_CLOSURE),
      Phase.scheduleGlobal    (CLEAR_BARRIER_ACTIVE),
      Phase.scheduleMutator   (CLEAR_BARRIER_ACTIVE),
      Phase.scheduleCollector (FINAL_MARK)
  );

  public final SharedDeque modbufPool = new SharedDeque("modBufs", metaDataSpace, 1);

  @Override
  @Interruptible
  public void processOptions() {
    super.processOptions();
    replacePhase(Phase.scheduleCollector(CLOSURE), Phase.scheduleComplex(concurrentClosure));
  }

  protected boolean inConcurrentCollection = false;

  @Override
  @Inline
  public void collectionPhase(short phaseId) {
    if (phaseId == SET_BARRIER_ACTIVE) {
      G1Mutator.newMutatorBarrierActive = true;
      return;
    }
    if (phaseId == CLEAR_BARRIER_ACTIVE) {
      G1Mutator.newMutatorBarrierActive = false;
      return;
    }
    if (phaseId == CLOSURE) {
      return;
    }
    if (phaseId == PREPARE) {
      inConcurrentCollection = true;
      modbufPool.prepareNonBlocking();
    }
    if (phaseId == RELEASE) {
      inConcurrentCollection = false;
      modbufPool.reset();//(1);
    }
    super.collectionPhase(phaseId);
  }

  @Override
  protected boolean concurrentCollectionRequired() {
    return !Phase.concurrentPhaseActive() &&
        ((getPagesReserved() * 100) / getTotalPages()) > 45;
  }

  final static Word LOG_MASK = Word.one().lsh(2);

  @Inline
  public static boolean attemptLog(ObjectReference o) {
    Word oldValue, newValue;
    do {
      oldValue = VM.objectModel.prepareAvailableBits(o);
      if (!oldValue.and(LOG_MASK).isZero()) return false;
      newValue = oldValue.or(LOG_MASK);
    } while (!VM.objectModel.attemptAvailableBits(o, oldValue, newValue));
    return true;
  }
}
