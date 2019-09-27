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

import org.mmtk.utility.deque.ObjectReferenceDeque;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.ObjectReference;
import org.vmmagic.unboxed.Offset;
import org.vmmagic.unboxed.Word;

import static org.mmtk.utility.Constants.BYTES_IN_ADDRESS;

@Uninterruptible
public class G1Mutator extends org.mmtk.plan.g1.baseline.G1Mutator {
  public static boolean newMutatorBarrierActive = false;
  protected volatile boolean barrierActive = false;
  private final ObjectReferenceDeque modbuf;

  protected G1Mutator() {
    barrierActive = newMutatorBarrierActive;
    modbuf = new ObjectReferenceDeque("modbuf", global().modbufPool);
  }

  /****************************************************************************
   *
   * Collection
   */

  /**
   * Perform a per-mutator collection phase.
   */
  @Override
  @Inline
  public void collectionPhase(short phaseId, boolean primary) {
    if (phaseId == G1.SET_BARRIER_ACTIVE) {
      barrierActive = true;
      return;
    }

    if (phaseId == G1.CLEAR_BARRIER_ACTIVE) {
      barrierActive = false;
      return;
    }

    if (phaseId == G1.FLUSH_MUTATOR) {
      flush();
      return;
    }

    super.collectionPhase(phaseId, primary);
  }

  @Override
  public void flushRememberedSets() {
    g1.reset();
    modbuf.flushLocal();
  }

  /****************************************************************************
   *
   * Write and read barriers.
   */

  /**
   * {@inheritDoc}<p>
   *
   * <b>In this case we employ a Yuasa style snapshot barrier.</b>
   *
   */
  @Inline
  @Override
  public void objectReferenceWrite(ObjectReference src, Address slot, ObjectReference tgt, Word metaDataA, Word metaDataB, int mode) {
    if (barrierActive) checkAndEnqueueReference(slot.loadObjectReference());
    VM.barriers.objectReferenceWrite(src, tgt, metaDataA, metaDataB, mode);
  }

  @Inline
  @Override
  public boolean objectReferenceTryCompareAndSwap(ObjectReference src, Address slot, ObjectReference old,
                                                  ObjectReference tgt, Word metaDataA, Word metaDataB, int mode) {
    boolean result = VM.barriers.objectReferenceTryCompareAndSwap(src, old, tgt, metaDataA, metaDataB, mode);
    if (barrierActive) checkAndEnqueueReference(old);
    return result;
  }

  /**
   * {@inheritDoc}
   *
   * @param src The source of the values to be copied
   * @param srcOffset The offset of the first source address, in
   * bytes, relative to <code>src</code> (in principle, this could be
   * negative).
   * @param dst The mutated object, i.e. the destination of the copy.
   * @param dstOffset The offset of the first destination address, in
   * bytes relative to <code>tgt</code> (in principle, this could be
   * negative).
   * @param bytes The size of the region being copied, in bytes.
   */
  @Inline
  @Override
  public boolean objectReferenceBulkCopy(ObjectReference src, Offset srcOffset, ObjectReference dst, Offset dstOffset, int bytes) {
    Address cursor = dst.toAddress().plus(dstOffset);
    Address limit = cursor.plus(bytes);
    while (cursor.LT(limit)) {
      ObjectReference ref = cursor.loadObjectReference();
      if (barrierActive) checkAndEnqueueReference(ref);
      cursor = cursor.plus(BYTES_IN_ADDRESS);
    }
    return false;
  }

  @Inline
  @Override
  public ObjectReference javaLangReferenceReadBarrier(ObjectReference ref) {
    if (barrierActive) checkAndEnqueueReference(ref);
    return ref;
  }

  /**
   * Process a reference that may require being enqueued as part of a concurrent
   * collection.
   *
   * @param ref The reference to check.
   */
  protected void checkAndEnqueueReference(ObjectReference ref) {
    if (!ref.isNull() && G1.attemptLog(ref)) {

    }
  }

  @Inline
  private static G1 global() {
    return (G1) VM.activePlan.global();
  }
}
