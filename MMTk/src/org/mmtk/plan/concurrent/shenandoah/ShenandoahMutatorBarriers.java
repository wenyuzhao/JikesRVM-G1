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
    ObjectReference newSrc = getForwardingPointer(src, true);
    Address newSlot = newSrc.toAddress().plus(metaDataA.toOffset());
    if (barrierActive) checkAndEnqueueReference(getForwardingPointer(newSlot.loadObjectReference(), false));
    VM.barriers.objectReferenceWrite(newSrc, getForwardingPointer(tgt, true), metaDataA, metaDataB, mode);
//    super.objectReferenceWrite(newSrc, newSlot, getForwardingPointer(tgt, false), metaDataA, metaDataB, mode);
  }

  @Override
  public boolean objectReferenceTryCompareAndSwap(ObjectReference src, Address slot, ObjectReference old, ObjectReference tgt,
                                                  Word metaDataA, Word metaDataB, int mode) {
//    return super.objectReferenceTryCompareAndSwap(getForwardingPointer(src, true), slot, old, getForwardingPointer(tgt, false), metaDataA, metaDataB, mode);
    boolean result = VM.barriers.objectReferenceTryCompareAndSwap(getForwardingPointer(src, true), old, getForwardingPointer(tgt, true), metaDataA, metaDataB, mode);
    if (barrierActive) checkAndEnqueueReference(getForwardingPointer(old, false));
    return result;
  }

  @Inline public boolean objectReferenceBulkCopy(ObjectReference src, Offset srcOffset, ObjectReference dst, Offset dstOffset, int bytes) {
//    super.objectReferenceBulkCopy(getForwardingPointer(src), srcOffset, getForwardingPointer(dst), dstOffset, bytes);
//    return bulkCopy(src, srcOffset, dst, dstOffset, bytes);
    Address srcCursor = getForwardingPointer(src, true).toAddress().plus(srcOffset);
    Address dstCursor = getForwardingPointer(dst, true).toAddress().plus(dstOffset);
    Address limit = dstCursor.plus(bytes);
    while (dstCursor.LT(limit)) {
      ObjectReference ref = getForwardingPointer(dstCursor.loadObjectReference(), false);
      if (barrierActive) checkAndEnqueueReference(ref);
      dstCursor.store(getForwardingPointer(srcCursor.loadObjectReference(), true));
      srcCursor = srcCursor.plus(Constants.BYTES_IN_ADDRESS);
      dstCursor = dstCursor.plus(Constants.BYTES_IN_ADDRESS);
    }
    return true;
  }

  @Inline public ObjectReference javaLangReferenceReadBarrier(ObjectReference ref) {
    return super.javaLangReferenceReadBarrier(getForwardingPointer(ref, true));
  }

  @Inline
  @Override
  public ObjectReference objectReferenceRead(ObjectReference src, Address slot, Word metaDataA, Word metaDataB, int mode) {
    return getForwardingPointer(VM.barriers.objectReferenceRead(getForwardingPointer(src, true), metaDataA, metaDataB, mode), false);
  }

  @Inline @Override public ObjectReference objectReferenceNonHeapRead(Address slot, Word metaDataA, Word metaDataB) {
    return getForwardingPointer(slot.loadObjectReference(), false);//global().loadObjectReference(slot);
  }

  @Inline @Override public void objectReferenceNonHeapWrite(Address slot, ObjectReference tgt, Word metaDataA, Word metaDataB) {
    VM.barriers.objectReferenceNonHeapWrite(slot, getForwardingPointer(tgt, true), metaDataA, metaDataB);
  }

  @Inline public boolean objectAddressCompare(ObjectReference lhs, ObjectReference rhs) {
//    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(false);
//    if (lhs.toAddress().EQ(rhs.toAddress())) return true;
    return getForwardingPointer(lhs, true).toAddress().EQ(getForwardingPointer(rhs, true).toAddress());
  }

  // Read barriers

//  @Inline public ObjectReference javaLangReferenceReadBarrier(ObjectReference referent) {
//    return getForwardingPointer(super.javaLangReferenceReadBarrier(getForwardingPointer(referent)));
//  }

  @Inline public boolean booleanRead(ObjectReference src, Address slot, Word metaDataA, Word metaDataB, int mode) {
    return VM.barriers.booleanRead(getForwardingPointer(src, true), metaDataA, metaDataB, mode);
  }

  @Inline public byte byteRead(ObjectReference src, Address slot, Word metaDataA, Word metaDataB, int mode) {
    return VM.barriers.byteRead(getForwardingPointer(src, true), metaDataA, metaDataB, mode);
  }

  @Inline public char charRead(ObjectReference src, Address slot, Word metaDataA, Word metaDataB, int mode) {
    return VM.barriers.charRead(getForwardingPointer(src, true), metaDataA, metaDataB, mode);
  }

  @Inline public short shortRead(ObjectReference src, Address slot, Word metaDataA, Word metaDataB, int mode) {
    return VM.barriers.shortRead(getForwardingPointer(src, true), metaDataA, metaDataB, mode);
  }

  @Inline public int intRead(ObjectReference src, Address slot, Word metaDataA, Word metaDataB, int mode) {
    return VM.barriers.intRead(getForwardingPointer(src, true), metaDataA, metaDataB, mode);
  }

  @Inline public long longRead(ObjectReference src, Address slot, Word metaDataA, Word metaDataB, int mode) {
    return VM.barriers.longRead(getForwardingPointer(src, true), metaDataA, metaDataB, mode);
  }

  @Inline public float floatRead(ObjectReference src, Address slot, Word metaDataA, Word metaDataB, int mode) {
    return VM.barriers.floatRead(getForwardingPointer(src, true), metaDataA, metaDataB, mode);
  }

  @Inline public double doubleRead(ObjectReference src, Address slot, Word metaDataA, Word metaDataB, int mode) {
    return VM.barriers.doubleRead(getForwardingPointer(src, true), metaDataA, metaDataB, mode);
  }

  @Inline public Word wordRead(ObjectReference src, Address slot, Word metaDataA, Word metaDataB, int mode) {
    return VM.barriers.wordRead(getForwardingPointer(src, true), metaDataA, metaDataB, mode);
  }

  @Inline public Address addressRead(ObjectReference src, Address slot, Word metaDataA, Word metaDataB, int mode) {
    return VM.barriers.addressRead(getForwardingPointer(src, true), metaDataA, metaDataB, mode);
  }

  @Inline public Extent extentRead(ObjectReference src, Address slot, Word metaDataA, Word metaDataB, int mode) {
    return VM.barriers.extentRead(getForwardingPointer(src, true), metaDataA, metaDataB, mode);
  }

  @Inline public Offset offsetRead(ObjectReference src, Address slot, Word metaDataA, Word metaDataB, int mode) {
    return VM.barriers.offsetRead(getForwardingPointer(src, true), metaDataA, metaDataB, mode);
  }

  // Write barriers

  @Inline public void booleanWrite(ObjectReference src, Address slot, boolean value, Word metaDataA, Word metaDataB, int mode) {
    VM.barriers.booleanWrite(getForwardingPointer(src, true), value, metaDataA, metaDataB, mode);
  }

  @Inline public void byteWrite(ObjectReference src, Address slot, byte value, Word metaDataA, Word metaDataB, int mode) {
    VM.barriers.byteWrite(getForwardingPointer(src, true), value, metaDataA, metaDataB, mode);
  }

  @Inline public void charWrite(ObjectReference src, Address slot, char value, Word metaDataA, Word metaDataB, int mode) {
    VM.barriers.charWrite(getForwardingPointer(src, true), value, metaDataA, metaDataB, mode);
  }

  @Inline public void shortWrite(ObjectReference src, Address slot, short value, Word metaDataA, Word metaDataB, int mode) {
    VM.barriers.shortWrite(getForwardingPointer(src, true), value, metaDataA, metaDataB, mode);
  }

  @Inline public void intWrite(ObjectReference src, Address slot, int value, Word metaDataA, Word metaDataB, int mode) {
    VM.barriers.intWrite(getForwardingPointer(src, true), value, metaDataA, metaDataB, mode);
  }

  @Inline public void longWrite(ObjectReference src, Address slot, long value, Word metaDataA, Word metaDataB, int mode) {
    VM.barriers.longWrite(getForwardingPointer(src, true), value, metaDataA, metaDataB, mode);
  }

  @Inline public void floatWrite(ObjectReference src, Address slot, float value, Word metaDataA, Word metaDataB, int mode) {
    VM.barriers.floatWrite(getForwardingPointer(src, true), value, metaDataA, metaDataB, mode);
  }

  @Inline public void doubleWrite(ObjectReference src, Address slot, double value, Word metaDataA, Word metaDataB, int mode) {
    VM.barriers.doubleWrite(getForwardingPointer(src, true), value, metaDataA, metaDataB, mode);
  }

  @Inline public void wordWrite(ObjectReference src, Address slot, Word value, Word metaDataA, Word metaDataB, int mode) {
    VM.barriers.wordWrite(getForwardingPointer(src, true), value, metaDataA, metaDataB, mode);
  }

  @Inline public void addressWrite(ObjectReference src, Address slot, Address value, Word metaDataA, Word metaDataB, int mode) {
    VM.barriers.addressWrite(getForwardingPointer(src, true), value, metaDataA, metaDataB, mode);
  }

  @Inline public void extentWrite(ObjectReference src, Address slot, Extent value, Word metaDataA, Word metaDataB, int mode) {
    VM.barriers.extentWrite(getForwardingPointer(src, true), value, metaDataA, metaDataB, mode);
  }

  @Inline public void offsetWrite(ObjectReference src, Address slot, Offset value, Word metaDataA, Word metaDataB, int mode) {
    VM.barriers.offsetWrite(getForwardingPointer(src, true), value, metaDataA, metaDataB, mode);
  }

  // CAS barriers

  @Inline public boolean intTryCompareAndSwap(ObjectReference src, Address slot, int old, int value, Word metaDataA, Word metaDataB, int mode) {
    return VM.barriers.intTryCompareAndSwap(getForwardingPointer(src, true), old, value, metaDataA, metaDataB, mode);
  }

  @Inline public boolean longTryCompareAndSwap(ObjectReference src, Address slot, long old, long value, Word metaDataA, Word metaDataB, int mode) {
    return VM.barriers.longTryCompareAndSwap(getForwardingPointer(src, true), old, value, metaDataA, metaDataB, mode);
  }

  @Inline public boolean wordTryCompareAndSwap(ObjectReference src, Address slot, Word old, Word value, Word metaDataA, Word metaDataB, int mode) {
    return VM.barriers.wordTryCompareAndSwap(getForwardingPointer(src, true), old, value, metaDataA, metaDataB, mode);
  }

  @Inline public boolean addressTryCompareAndSwap(ObjectReference src, Address slot, Address old, Address value, Word metaDataA, Word metaDataB, int mode) {
    return VM.barriers.addressTryCompareAndSwap(getForwardingPointer(src, true), old, value, metaDataA, metaDataB, mode);
  }

  // Bulk copy barriers

  @Inline private boolean bulkCopy(ObjectReference src, Offset srcOffset, ObjectReference dst, Offset dstOffset, int bytes) {
    VM.memory.copy(getForwardingPointer(src, true).toAddress().plus(srcOffset), getForwardingPointer(dst, true).toAddress().plus(dstOffset), bytes);
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
