package org.mmtk.plan.concurrent.shenandoah;

import org.mmtk.plan.concurrent.ConcurrentMutator;
import org.mmtk.utility.Constants;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.*;

import static org.mmtk.plan.concurrent.shenandoah.Shenandoah.getForwardingPointer;

@Uninterruptible
public abstract class ShenandoahMutatorBarriers extends ConcurrentMutator {
  @Inline
  @Override
  public void objectReferenceWrite(ObjectReference src, Address slot, ObjectReference tgt, Word metaDataA, Word metaDataB, int mode) {
    ObjectReference newSrc = getForwardingPointer(src);
    Address newSlot = newSrc.toAddress().plus(metaDataA.toOffset());
    if (barrierActive) checkAndEnqueueReference(getForwardingPointer(newSlot.loadObjectReference()));
    VM.barriers.objectReferenceWrite(newSrc, getForwardingPointer(tgt), metaDataA, metaDataB, mode);
//    super.objectReferenceWrite(newSrc, newSlot, getForwardingPointer(tgt, false), metaDataA, metaDataB, mode);
  }

  @Override
  public boolean objectReferenceTryCompareAndSwap(ObjectReference src, Address slot, ObjectReference old, ObjectReference tgt, Word metaDataA, Word metaDataB, int mode) {
    ObjectReference newTarget = getForwardingPointer(tgt);
    ObjectReference newSrc = getForwardingPointer(tgt);
    boolean result = VM.barriers.objectReferenceTryCompareAndSwap(newSrc, old, newTarget, metaDataA, metaDataB, mode);
    if (!result) {
      result = VM.barriers.objectReferenceTryCompareAndSwap(newSrc, getForwardingPointer(old), newTarget, metaDataA, metaDataB, mode);
    }
    if (barrierActive) checkAndEnqueueReference(old);
    return result;
  }

  @Inline public boolean objectReferenceBulkCopy(ObjectReference src, Offset srcOffset, ObjectReference dst, Offset dstOffset, int bytes) {
    Address srcCursor = getForwardingPointer(src).toAddress().plus(srcOffset);
    Address dstCursor = getForwardingPointer(dst).toAddress().plus(dstOffset);
    Address limit = dstCursor.plus(bytes);
    while (dstCursor.LT(limit)) {
      if (barrierActive) checkAndEnqueueReference(dstCursor.loadObjectReference());
      dstCursor.store(getForwardingPointer(srcCursor.loadObjectReference()));
      srcCursor = srcCursor.plus(Constants.BYTES_IN_ADDRESS);
      dstCursor = dstCursor.plus(Constants.BYTES_IN_ADDRESS);
    }
    return true;
  }

  @Inline public ObjectReference javaLangReferenceReadBarrier(ObjectReference ref) {
    return super.javaLangReferenceReadBarrier(getForwardingPointer(ref));
  }

  @Inline
  @Override
  public ObjectReference objectReferenceRead(ObjectReference src, Address slot, Word metaDataA, Word metaDataB, int mode) {
    return getForwardingPointer(VM.barriers.objectReferenceRead(getForwardingPointer(src), metaDataA, metaDataB, mode));
  }

  @Inline @Override public ObjectReference objectReferenceNonHeapRead(Address slot, Word metaDataA, Word metaDataB) {
    return getForwardingPointer(slot.loadObjectReference());//global().loadObjectReference(slot);
  }

  @Inline @Override public void objectReferenceNonHeapWrite(Address slot, ObjectReference tgt, Word metaDataA, Word metaDataB) {
    VM.barriers.objectReferenceNonHeapWrite(slot, getForwardingPointer(tgt), metaDataA, metaDataB);
  }

  @Inline public boolean objectAddressCompare(ObjectReference lhs, ObjectReference rhs) {
    if (lhs.toAddress().NE(rhs.toAddress()) && getForwardingPointer(lhs).toAddress().NE(getForwardingPointer(rhs).toAddress())) {
      return false;
    } else {
      return true;
    }
  }

  // Read barriers

//  @Inline public ObjectReference javaLangReferenceReadBarrier(ObjectReference referent) {
//    return getForwardingPointer(super.javaLangReferenceReadBarrier(getForwardingPointer(referent)));
//  }

  @Inline public boolean booleanRead(ObjectReference src, Address slot, Word metaDataA, Word metaDataB, int mode) {
    return VM.barriers.booleanRead(getForwardingPointer(src), metaDataA, metaDataB, mode);
  }

  @Inline public byte byteRead(ObjectReference src, Address slot, Word metaDataA, Word metaDataB, int mode) {
    return VM.barriers.byteRead(getForwardingPointer(src), metaDataA, metaDataB, mode);
  }

  @Inline public char charRead(ObjectReference src, Address slot, Word metaDataA, Word metaDataB, int mode) {
    return VM.barriers.charRead(getForwardingPointer(src), metaDataA, metaDataB, mode);
  }

  @Inline public short shortRead(ObjectReference src, Address slot, Word metaDataA, Word metaDataB, int mode) {
    return VM.barriers.shortRead(getForwardingPointer(src), metaDataA, metaDataB, mode);
  }

  @Inline public int intRead(ObjectReference src, Address slot, Word metaDataA, Word metaDataB, int mode) {
    return VM.barriers.intRead(getForwardingPointer(src), metaDataA, metaDataB, mode);
  }

  @Inline public long longRead(ObjectReference src, Address slot, Word metaDataA, Word metaDataB, int mode) {
    return VM.barriers.longRead(getForwardingPointer(src), metaDataA, metaDataB, mode);
  }

  @Inline public float floatRead(ObjectReference src, Address slot, Word metaDataA, Word metaDataB, int mode) {
    return VM.barriers.floatRead(getForwardingPointer(src), metaDataA, metaDataB, mode);
  }

  @Inline public double doubleRead(ObjectReference src, Address slot, Word metaDataA, Word metaDataB, int mode) {
    return VM.barriers.doubleRead(getForwardingPointer(src), metaDataA, metaDataB, mode);
  }

  @Inline public Word wordRead(ObjectReference src, Address slot, Word metaDataA, Word metaDataB, int mode) {
    return VM.barriers.wordRead(getForwardingPointer(src), metaDataA, metaDataB, mode);
  }

  @Inline public Address addressRead(ObjectReference src, Address slot, Word metaDataA, Word metaDataB, int mode) {
    return VM.barriers.addressRead(getForwardingPointer(src), metaDataA, metaDataB, mode);
  }

  @Inline public Extent extentRead(ObjectReference src, Address slot, Word metaDataA, Word metaDataB, int mode) {
    return VM.barriers.extentRead(getForwardingPointer(src), metaDataA, metaDataB, mode);
  }

  @Inline public Offset offsetRead(ObjectReference src, Address slot, Word metaDataA, Word metaDataB, int mode) {
    return VM.barriers.offsetRead(getForwardingPointer(src), metaDataA, metaDataB, mode);
  }

  // Write barriers

  @Inline public void booleanWrite(ObjectReference src, Address slot, boolean value, Word metaDataA, Word metaDataB, int mode) {
    VM.barriers.booleanWrite(getForwardingPointer(src), value, metaDataA, metaDataB, mode);
  }

  @Inline public void byteWrite(ObjectReference src, Address slot, byte value, Word metaDataA, Word metaDataB, int mode) {
    VM.barriers.byteWrite(getForwardingPointer(src), value, metaDataA, metaDataB, mode);
  }

  @Inline public void charWrite(ObjectReference src, Address slot, char value, Word metaDataA, Word metaDataB, int mode) {
    VM.barriers.charWrite(getForwardingPointer(src), value, metaDataA, metaDataB, mode);
  }

  @Inline public void shortWrite(ObjectReference src, Address slot, short value, Word metaDataA, Word metaDataB, int mode) {
    VM.barriers.shortWrite(getForwardingPointer(src), value, metaDataA, metaDataB, mode);
  }

  @Inline public void intWrite(ObjectReference src, Address slot, int value, Word metaDataA, Word metaDataB, int mode) {
    VM.barriers.intWrite(getForwardingPointer(src), value, metaDataA, metaDataB, mode);
  }

  @Inline public void longWrite(ObjectReference src, Address slot, long value, Word metaDataA, Word metaDataB, int mode) {
    VM.barriers.longWrite(getForwardingPointer(src), value, metaDataA, metaDataB, mode);
  }

  @Inline public void floatWrite(ObjectReference src, Address slot, float value, Word metaDataA, Word metaDataB, int mode) {
    VM.barriers.floatWrite(getForwardingPointer(src), value, metaDataA, metaDataB, mode);
  }

  @Inline public void doubleWrite(ObjectReference src, Address slot, double value, Word metaDataA, Word metaDataB, int mode) {
    VM.barriers.doubleWrite(getForwardingPointer(src), value, metaDataA, metaDataB, mode);
  }

  @Inline public void wordWrite(ObjectReference src, Address slot, Word value, Word metaDataA, Word metaDataB, int mode) {
    VM.barriers.wordWrite(getForwardingPointer(src), value, metaDataA, metaDataB, mode);
  }

  @Inline public void addressWrite(ObjectReference src, Address slot, Address value, Word metaDataA, Word metaDataB, int mode) {
    VM.barriers.addressWrite(getForwardingPointer(src), value, metaDataA, metaDataB, mode);
  }

  @Inline public void extentWrite(ObjectReference src, Address slot, Extent value, Word metaDataA, Word metaDataB, int mode) {
    VM.barriers.extentWrite(getForwardingPointer(src), value, metaDataA, metaDataB, mode);
  }

  @Inline public void offsetWrite(ObjectReference src, Address slot, Offset value, Word metaDataA, Word metaDataB, int mode) {
    VM.barriers.offsetWrite(getForwardingPointer(src), value, metaDataA, metaDataB, mode);
  }

  // CAS barriers

  @Inline public boolean intTryCompareAndSwap(ObjectReference src, Address slot, int old, int value, Word metaDataA, Word metaDataB, int mode) {
    return VM.barriers.intTryCompareAndSwap(getForwardingPointer(src), old, value, metaDataA, metaDataB, mode);
  }

  @Inline public boolean longTryCompareAndSwap(ObjectReference src, Address slot, long old, long value, Word metaDataA, Word metaDataB, int mode) {
    return VM.barriers.longTryCompareAndSwap(getForwardingPointer(src), old, value, metaDataA, metaDataB, mode);
  }

  @Inline public boolean wordTryCompareAndSwap(ObjectReference src, Address slot, Word old, Word value, Word metaDataA, Word metaDataB, int mode) {
    return VM.barriers.wordTryCompareAndSwap(getForwardingPointer(src), old, value, metaDataA, metaDataB, mode);
  }

  @Inline public boolean addressTryCompareAndSwap(ObjectReference src, Address slot, Address old, Address value, Word metaDataA, Word metaDataB, int mode) {
    return VM.barriers.addressTryCompareAndSwap(getForwardingPointer(src), old, value, metaDataA, metaDataB, mode);
  }

  // Bulk copy barriers

  @Inline private boolean bulkCopy(ObjectReference src, Offset srcOffset, ObjectReference dst, Offset dstOffset, int bytes) {
    VM.memory.copy(getForwardingPointer(src).toAddress().plus(srcOffset), getForwardingPointer(dst).toAddress().plus(dstOffset), bytes);
    return true;
  }

  @Inline public boolean booleanBulkCopy(ObjectReference src, Offset srcOffset, ObjectReference dst, Offset dstOffset, int bytes) {
    return bulkCopy(src, srcOffset, dst, dstOffset, bytes);
  }

  @Inline public boolean byteBulkCopy(ObjectReference src, Offset srcOffset, ObjectReference dst, Offset dstOffset, int bytes) {
    return bulkCopy(src, srcOffset, dst, dstOffset, bytes);
  }

  @Inline public boolean charBulkCopy(ObjectReference src, Offset srcOffset, ObjectReference dst, Offset dstOffset, int bytes) {
    return bulkCopy(src, srcOffset, dst, dstOffset, bytes);
  }

  @Inline public boolean shortBulkCopy(ObjectReference src, Offset srcOffset, ObjectReference dst, Offset dstOffset, int bytes) {
    return bulkCopy(src, srcOffset, dst, dstOffset, bytes);
  }

  @Inline public boolean intBulkCopy(ObjectReference src, Offset srcOffset, ObjectReference dst, Offset dstOffset, int bytes) {
    return bulkCopy(src, srcOffset, dst, dstOffset, bytes);
  }

  @Inline public boolean longBulkCopy(ObjectReference src, Offset srcOffset, ObjectReference dst, Offset dstOffset, int bytes) {
    return bulkCopy(src, srcOffset, dst, dstOffset, bytes);
  }

  @Inline public boolean floatBulkCopy(ObjectReference src, Offset srcOffset, ObjectReference dst, Offset dstOffset, int bytes) {
    return bulkCopy(src, srcOffset, dst, dstOffset, bytes);
  }

  @Inline public boolean doubleBulkCopy(ObjectReference src, Offset srcOffset, ObjectReference dst, Offset dstOffset, int bytes) {
    return bulkCopy(src, srcOffset, dst, dstOffset, bytes);
  }

  @Inline public boolean wordBulkCopy(ObjectReference src, Offset srcOffset, ObjectReference dst, Offset dstOffset, int bytes) {
    return bulkCopy(src, srcOffset, dst, dstOffset, bytes);
  }

  @Inline public boolean addressBulkCopy(ObjectReference src, Offset srcOffset, ObjectReference dst, Offset dstOffset, int bytes) {
    return bulkCopy(src, srcOffset, dst, dstOffset, bytes);
  }

  @Inline public boolean extentBulkCopy(ObjectReference src, Offset srcOffset, ObjectReference dst, Offset dstOffset, int bytes) {
    return bulkCopy(src, srcOffset, dst, dstOffset, bytes);
  }

  @Inline public boolean offsetBulkCopy(ObjectReference src, Offset srcOffset, ObjectReference dst, Offset dstOffset, int bytes) {
    return bulkCopy(src, srcOffset, dst, dstOffset, bytes);
  }

  @Inline Shenandoah global() {
    return (Shenandoah) VM.activePlan.global();
  }
}
