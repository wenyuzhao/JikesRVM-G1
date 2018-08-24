package org.mmtk.policy;

import org.mmtk.plan.Plan;
import org.mmtk.utility.Constants;
import org.mmtk.utility.Log;
import org.mmtk.vm.Memory;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.*;

@Uninterruptible
public class ForwardingTable {
  private static final int LOG_BYTES_IN_FT = 18;
  private static final int LOG_PAGES_IN_FT = LOG_BYTES_IN_FT - Constants.LOG_BYTES_IN_PAGE;
  private static final int PAGES_IN_FT = 1 << LOG_PAGES_IN_FT;
  private static final Extent EXTENT = Extent.fromIntSignExtend(1 << LOG_BYTES_IN_FT);

  @Inline
  public static void clear(Address region) {
    Address tablePointer = Region.metaDataOf(region, Region.METADATA_FORWARDING_TABLE_OFFSET);
    Address table = tablePointer.loadAddress();
    if (!table.isZero()) {
      Plan.metaDataSpace.release(table);
    }
    tablePointer.store(Address.zero());
  }

  @Inline
  private static int computeIndex(ObjectReference ref) {
    Word index = ref.toAddress().diff(Region.of(ref)).toWord().rsha(4);
    if (VM.VERIFY_ASSERTIONS) {
      VM.assertions._assert(index.lsh(2).LT(EXTENT.toWord()));
    }
    return index.toInt();
  }

  @Inline
  public static final ObjectReference getForwardingPointer(ObjectReference ref) {
//    Log.writeln("getForwardingPointer ", ref);
    Address region = Region.of(ref);
    Address table = Region.metaDataOf(region, Region.METADATA_FORWARDING_TABLE_OFFSET).loadAddress();
    if (table.isZero()) return ObjectReference.nullReference();
    int index = computeIndex(ref);
    ObjectReference newRef = table.loadObjectReference(Offset.fromIntZeroExtend(index << 2));
//    if (!newRef.isNull()) {
//      Log.write(ref);
//      Log.writeln(" ~> ", newRef);
//    }
    return newRef.isNull() ? ObjectReference.nullReference() : newRef;
  }

  @Inline
  public static final void setForwardingPointer(ObjectReference oldRef, ObjectReference forwardedRef) {
    Address region = Region.of(oldRef);
    Address tablePointer = Region.metaDataOf(region, Region.METADATA_FORWARDING_TABLE_OFFSET);
    if (tablePointer.loadAddress().isZero()) {
      tablePointer.store(Plan.metaDataSpace.acquire(PAGES_IN_FT));
    }
    Address table = tablePointer.loadAddress();
    int index = computeIndex(oldRef);
    if (VM.VERIFY_ASSERTIONS) {
      VM.assertions._assert(table.loadObjectReference(Offset.fromIntZeroExtend(index << 2)).isNull());
    }
    table.store(forwardedRef, Offset.fromIntZeroExtend(index << 2));
    if (VM.VERIFY_ASSERTIONS) {
      VM.assertions._assert(VM.debugging.validRef(oldRef));
      VM.assertions._assert(VM.debugging.validRef(forwardedRef));
      VM.assertions._assert(forwardedRef.toAddress().EQ(getForwardingPointer(oldRef).toAddress()));
    }
  }
}
