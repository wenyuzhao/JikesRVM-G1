package org.mmtk.policy.region;

import org.mmtk.plan.g1.G1;
import org.mmtk.utility.Constants;
import org.mmtk.utility.alloc.BumpPointer2;
import org.mmtk.utility.heap.layout.HeapLayout;
import org.mmtk.utility.heap.layout.VMLayoutConstants;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.ObjectReference;
import org.vmmagic.unboxed.Word;

@Uninterruptible
public class Card {
  public static final int LOG_BYTES_IN_CARD = 9;
  public static final int BYTES_IN_CARD = 1 << LOG_BYTES_IN_CARD;
  public static final int CARDS_IN_HEAP = 1 << (VMLayoutConstants.LOG_ADDRESS_SPACE - LOG_BYTES_IN_CARD);//.HEAP_END.diff(VM.HEAP_START).toWord().rshl(LOG_BYTES_IN_CARD).toInt();
  public static final int CARDS_IN_REGION = Word.fromIntZeroExtend(Region.BYTES_IN_REGION).rshl(LOG_BYTES_IN_CARD).toInt();
  public static final Word CARD_MASK = Word.fromIntZeroExtend(BYTES_IN_CARD - 1);// 0..0111111111

  public static final byte NOT_DIRTY = 0;
  public static final byte DIRTY = 1;
  public static final byte NURSERY = 2;

  @Inline
  public static Address of(Address address) {
    return address.toWord().and(CARD_MASK.not()).toAddress();
  }

  @Inline
  public static Address of(ObjectReference ref) {
    return of(VM.objectModel.objectStartRef(ref));
  }

  @Inline
  public static boolean isAligned(Address card) {
    return of(card).EQ(card);
  }

  @Inline
  public static int indexOf(Address card) {
    Address region = Region.of(card);
    int index = card.diff(region).toInt() >>> LOG_BYTES_IN_CARD;
    return index;
  }

  @Inline
  public static void linearScan(Address card, LinearScan linearScan, boolean markDead, Object context) {
//    if (!Space.isMappedAddress(card)) return;
    final int descriptor = HeapLayout.vmMap.getDescriptorForAddress(card);
    if (descriptor == G1.REGION_SPACE) {
      Address region = Region.of(card);
//      if (!Region.getBool(region, Region.MD_ALLOCATED)) return;
      if (Region.getBool(region, Region.MD_RELOCATE)) return;
      linearScanG1Card(card, linearScan, context, markDead);
    } else if (descriptor == G1.LOS) {
      linearScanLOSCard(card, linearScan, context);
    } else if (descriptor == G1.IMMORTAL) {
      BumpPointer2.linearScan(card, linearScan, markDead, context);
    }
  }

  static final int LOS_HEADER_SIZE = G1.loSpace.getHeaderSize();

  @Inline
  private static void linearScanLOSCard(Address card, LinearScan linearScan, Object context) {
    Address objectStartRef = card.plus(LOS_HEADER_SIZE);
    ObjectReference o = objectStartRef.plus(OBJECT_REF_OFFSET).toObjectReference();
    if (G1.loSpace.isLive(o)) {
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(VM.debugging.validRef(o));
      linearScan.scan(card, o, context);
    }
  }

  @Inline
  private static void linearScanG1Card(Address card, LinearScan linearScan, Object context, boolean log) {
    Address region = Region.of(card);
    Address cursor = CardOffsetTable.blockStart(region, card);
    if (cursor.isZero()) return;
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(cursor.GE(card));
    Address limit = card.plus(BYTES_IN_CARD);
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(card.LE(cursor));
    while (cursor.LT(limit)) {
      ObjectReference object = getObjectFromStartAddress(cursor, limit);
      if (object.isNull()) break;
      linearScan.scan(card, object, context);
      cursor = VM.objectModel.getObjectEndAddress(object);
    }
  }

  static final int OBJECT_REF_OFFSET = VM.objectModel.getObjectRefOffset();

  @Inline
  public static ObjectReference getObjectFromStartAddress(Address start, Address limit) {
    Address cursor = start;
    if (cursor.GE(limit)) return ObjectReference.nullReference();
    /* Skip over any alignment fill */
    while (cursor.loadInt() == Constants.ALIGNMENT_VALUE) {
      cursor = cursor.plus(Constants.BYTES_IN_WORD);
      if (cursor.GE(limit)) return ObjectReference.nullReference();
    }
    ObjectReference object = cursor.plus(OBJECT_REF_OFFSET).toObjectReference();
    if (BumpPointer2.tibIsZero(object)) return ObjectReference.nullReference();
    return object;
  }

  @Uninterruptible
  public static abstract class LinearScan<T> {
    public abstract void scan(Address card, ObjectReference object, T context);
  }
}
