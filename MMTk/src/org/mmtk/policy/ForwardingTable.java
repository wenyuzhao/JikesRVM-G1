package org.mmtk.policy;

import org.mmtk.plan.Plan;
import org.mmtk.utility.Constants;
import org.mmtk.utility.Log;
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

  private static final AddressArray forwardingTables;

  static {
    int regions = VM.AVAILABLE_END.diff(VM.AVAILABLE_START).toWord().rshl(Region.LOG_BYTES_IN_REGION).toInt();
    forwardingTables = AddressArray.create(regions);
  }

  @Inline
  public static void clear(Address region) {
    int index = regionIndex(region);
    Address table = forwardingTables.get(index);
    if (!table.isZero()) {
      Plan.metaDataSpace.release(table);
    }
    forwardingTables.set(index, Address.zero());
  }

  @Inline
  private static int regionIndex(Address region) {
    return region.diff(VM.AVAILABLE_START).toWord().rshl(Region.LOG_BYTES_IN_REGION).toInt();
  }

  @Inline
  private static int computeIndex(ObjectReference ref) {
    Word index = VM.objectModel.objectStartRef(ref).diff(Region.of(ref)).toWord().rsha(4);
    if (VM.VERIFY_ASSERTIONS) {
      if (!index.lsh(2).LT(EXTENT.toWord())) {
        VM.objectModel.dumpObject(ref);
        Log.writeln("Invalid ref ", Region.of(ref));
        Log.writeln("Region ", ref);
        Log.writeln("Index ", index);
        Log.writeln("EXTENT ", EXTENT);
        VM.assertions.fail("");
      }
      VM.assertions._assert(index.lsh(2).LT(EXTENT.toWord()));
    }
    return index.toInt();
  }

  @Inline
  public static final ObjectReference getForwardingPointer(ObjectReference ref) {
    VM.assertions._assert(VM.debugging.validRef(ref));
    int rIndex = regionIndex(Region.of(ref));
    Address table = forwardingTables.get(rIndex);
    if (table.isZero()) return ObjectReference.nullReference();
    int index = computeIndex(ref);
    ObjectReference newRef = table.loadObjectReference(Offset.fromIntZeroExtend(index << 2));
    return newRef.isNull() ? ObjectReference.nullReference() : newRef;
  }

  @Inline
  public static final void setForwardingPointer(ObjectReference oldRef, ObjectReference forwardedRef) {
    VM.assertions._assert(VM.debugging.validRef(oldRef));
    VM.assertions._assert(VM.debugging.validRef(forwardedRef));
    int rIndex = regionIndex(Region.of(oldRef));
    Address table = forwardingTables.get(rIndex);
    if (table.isZero()) {
      table = Plan.metaDataSpace.acquire(PAGES_IN_FT);
      forwardingTables.set(rIndex, table);
    }
    int index = computeIndex(oldRef);
    table.store(forwardedRef, Offset.fromIntZeroExtend(index << 2));
  }
}
