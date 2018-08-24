package org.mmtk.plan.concurrent.shenandoah;

import org.mmtk.plan.concurrent.ConcurrentMutator;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.*;

@Uninterruptible
public abstract class ShenandoahMutatorBarriers extends ConcurrentMutator {
  @Inline ObjectReference getForwardingPointer(ObjectReference obj) {
    return Shenandoah.getForwardingPointer(obj);
  }

  @Inline
  @Override
  public void objectReferenceWrite(ObjectReference src, Address slot, ObjectReference tgt, Word metaDataA, Word metaDataB, int mode) {
    ObjectReference newSrc = getForwardingPointer(src);
    Address newSlot = newSrc.toAddress().plus(metaDataA.toOffset());
    super.objectReferenceWrite(newSrc, newSlot, getForwardingPointer(tgt), metaDataA, metaDataB, mode);
//    VM.barriers.objectReferenceWrite();
  }

  @Override
  public boolean objectReferenceTryCompareAndSwap(ObjectReference src, Address slot, ObjectReference old, ObjectReference tgt,
                                                  Word metaDataA, Word metaDataB, int mode) {
    if (VM.VERIFY_ASSERTIONS) {
      VM.assertions._assert(old.toAddress().EQ(getForwardingPointer(old).toAddress()));
    }
    boolean result = VM.barriers.objectReferenceTryCompareAndSwap(getForwardingPointer(src), old, getForwardingPointer(tgt), metaDataA, metaDataB, mode);
    if (barrierActive) checkAndEnqueueReference(getForwardingPointer(old));
    return result;
//    return super.objectReferenceTryCompareAndSwap(getForwardingPointer(src), slot, old, getForwardingPointer(tgt), metaDataA, metaDataB, mode);
  }

  @Inline
  @Override
  public ObjectReference objectReferenceRead(ObjectReference src, Address slot, Word metaDataA, Word metaDataB, int mode) {
//    if (!src.isNull()) Space.isInSpace(Shenandoah.RS, src);
//    if (Shenandoah.relocationSetIsNull) {
//    if (!Plan.initialized) {
//      return VM.barriers.objectReferenceRead(src, metaDataA, metaDataB, mode);
//    }
    return getForwardingPointer(VM.barriers.objectReferenceRead(getForwardingPointer(src), metaDataA, metaDataB, mode));
//    return Poisoned.depoison(VM.barriers.wordRead(src, metaDataA, metaDataB, mode));
  }

  // Read barriers

  @Inline public ObjectReference javaLangReferenceReadBarrier(ObjectReference referent) {
    return getForwardingPointer(super.javaLangReferenceReadBarrier(getForwardingPointer(referent)));
  }

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

//  @Inline public ObjectReference objectReferenceRead(ObjectReference src, Address slot, Word metaDataA, Word metaDataB, int mode) {
//    return getForwardingPointer(VM.barriers.objectReferenceRead(getForwardingPointer(src), metaDataA, metaDataB, mode));
//  }

  @Inline public ObjectReference objectReferenceNonHeapRead(Address slot, Word metaDataA, Word metaDataB) {
    return getForwardingPointer(slot.loadObjectReference());//global().loadObjectReference(slot);
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

//  @Inline public void objectReferenceWrite(ObjectReference src, Address slot, ObjectReference value, Word metaDataA, Word metaDataB, int mode) {
//    super.objectReferenceWrite(getForwardingPointer(src), slot, getForwardingPointer(value), metaDataA, metaDataB, mode);
//  }

  @Inline public void objectReferenceNonHeapWrite(Address slot, ObjectReference tgt, Word metaDataA, Word metaDataB) {
    VM.barriers.objectReferenceNonHeapWrite(slot, getForwardingPointer(tgt), metaDataA, metaDataB);
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

//  @Inline public boolean objectReferenceTryCompareAndSwap(ObjectReference src, Address slot, ObjectReference old, ObjectReference tgt, Word metaDataA, Word metaDataB, int mode) {
//    return super.objectReferenceTryCompareAndSwap(getForwardingPointer(src), slot, getForwardingPointer(old), getForwardingPointer(tgt), metaDataA, metaDataB, mode);
//  }

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

  @Inline public boolean objectReferenceBulkCopy(ObjectReference src, Offset srcOffset, ObjectReference dst, Offset dstOffset, int bytes) {
    super.objectReferenceBulkCopy(getForwardingPointer(src), srcOffset, getForwardingPointer(dst), dstOffset, bytes);
    return bulkCopy(src, srcOffset, dst, dstOffset, bytes);
  }


  @Inline Shenandoah global() {
    return (Shenandoah) VM.activePlan.global();
  }
}
