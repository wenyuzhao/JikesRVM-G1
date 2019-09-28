package org.mmtk.policy.region;


//import org.mmtk.policy.*;
import org.mmtk.utility.Constants;
import org.mmtk.utility.Log;
import org.mmtk.utility.alloc.EmbeddedMetaData;
import org.mmtk.utility.alloc.LinearScan;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.*;

//import static org.mmtk.policy.region.Region.Card.OBJECT_END_ADDRESS_OFFSET;
import static org.mmtk.utility.Constants.*;

@Uninterruptible
public class Region {
//  public static final boolean VERBOSE;
  @Inline
  public static final boolean verbose() {
    return false;
//    return VM.VERIFY_ASSERTIONS && Options.verbose.getValue() != 0;
//    return Options.verbose.getValue() != 0;
  }

  // Generation information
  public static final int NORMAL = 0;
  public static final int EDEN = NORMAL;
  public static final int SURVIVOR = 1;
  public static final int OLD = 2;

  // Region size
//  public static final boolean USE_PLAN_SPECIFIC_REGION_SIZE = true;

  public static final int LOG_PAGES_IN_REGION = VM.activePlan.constraints().LOG_PAGES_IN_G1_REGION();
  public static final int PAGES_IN_REGION = 1 << LOG_PAGES_IN_REGION;
  public static final int LOG_BYTES_IN_REGION = LOG_PAGES_IN_REGION + LOG_BYTES_IN_PAGE;
//  public static final Word REGION_MASK = (1 << LOG_BYTES_IN_REGION) - 1;
  public static final int BYTES_IN_REGION = 1 << LOG_BYTES_IN_REGION;
  public static final int MAX_ALLOC_SIZE = (int) (BYTES_IN_REGION * 3 / 4);
  public static final int REGIONS_IN_CHUNK = (1 << (EmbeddedMetaData.LOG_PAGES_IN_REGION - LOG_PAGES_IN_REGION)) - 1;

//  public static final int LOG_TLABS_IN_REGION = LOG_BYTES_IN_REGION - LOG_BYTES_IN_TLAB;
//  public static final int TLABS_IN_REGION = 1 << LOG_TLABS_IN_REGION;


  private static final int INITIAL_LOG_BYTES_IN_TLAB = 11;
  public static final int MIN_TLAB_SIZE = 1 << INITIAL_LOG_BYTES_IN_TLAB;
  public static final int MAX_TLAB_SIZE = BYTES_IN_REGION;
//  public static int BYTES_IN_TLAB = MIN_TLAB_SIZE;

  public static final Word REGION_MASK = Word.fromIntZeroExtend(BYTES_IN_REGION - 1);// 0..011111111111
//  private static final Word TLAB_MASK = Word.fromIntZeroExtend(BYTES_IN_TLAB - 1);

  // Mark table:
  // 1 bit per 4 byte: 1/32 ratio
  // 4M MMTk block ~> 128kb (32 pages)

  // Metadata for each region
  // 1. (4b) Alive size
  // 2. (2b) Relocate state
  // 3. (2b) Allocate state
  // 4. (4b) Cursor offset
  // 5. (4b) Remset lock
  // 6. (4b) Remset size
  // 6. (4b) Remset pages
  // 7. (4b) Remset pointer
  // 8. (4b) Generation state
  // --- Total 32 bytes ---

  // Metadata layout:
  // Total 17 pages:
  // Page 0-15: mark table
  // Page 16: Per region metadata


  // Mark table
  private static final int LOG_PAGES_IN_MARKTABLE = 5;
  public static final int BYTES_IN_MARKTABLE = 1 << (LOG_PAGES_IN_MARKTABLE + LOG_BYTES_IN_PAGE);
  public static final int MARK_BYTES_PER_REGION = BYTES_IN_MARKTABLE / (REGIONS_IN_CHUNK + 1);
  // Per region metadata
  private static final int METADATA_ALIVE_SIZE_OFFSET = 0;
  private static final int METADATA_RELOCATE_OFFSET = METADATA_ALIVE_SIZE_OFFSET + BYTES_IN_INT;
  private static final int METADATA_ALLOCATED_OFFSET = METADATA_RELOCATE_OFFSET + BYTES_IN_SHORT;//BYTES_IN_BYTE;
  public static final int METADATA_PREV_CURSOR_OFFSET = METADATA_ALLOCATED_OFFSET + BYTES_IN_SHORT;//BYTES_IN_BYTE;
  public static final int METADATA_NEXT_CURSOR_OFFSET = METADATA_PREV_CURSOR_OFFSET + BYTES_IN_ADDRESS;//BYTES_IN_BYTE;
  public static final int METADATA_REMSET_LOCK_OFFSET = METADATA_NEXT_CURSOR_OFFSET + BYTES_IN_ADDRESS;
  public static final int METADATA_REMSET_SIZE_OFFSET = METADATA_REMSET_LOCK_OFFSET + BYTES_IN_INT;
  public static final int METADATA_REMSET_PAGES_OFFSET = METADATA_REMSET_SIZE_OFFSET + BYTES_IN_INT;
  public static final int METADATA_REMSET_POINTER_OFFSET = METADATA_REMSET_PAGES_OFFSET + BYTES_IN_INT;
  public static final int METADATA_GENERATION_OFFSET = METADATA_REMSET_POINTER_OFFSET + BYTES_IN_ADDRESS;
//  public static final int METADATA_TAMS_OFFSET = METADATA_GENERATION_OFFSET + BYTES_IN_ADDRESS;
  private static final int PER_REGION_METADATA_BYTES = METADATA_GENERATION_OFFSET + BYTES_IN_ADDRESS;
  private static final int PER_REGION_META_START_OFFSET = BYTES_IN_MARKTABLE;

  public static final int METADATA_PAGES_PER_CHUNK = (1 << LOG_PAGES_IN_MARKTABLE)  + 1;

  static {
//    if (VM.VERIFY_ASSERTIONS) {
//      VM.assertions._assert(LOG_PAGES_IN_REGION >= 4 && LOG_PAGES_IN_REGION <= 8);
//      VM.assertions._assert(PER_REGION_METADATA_BYTES == 40);
//    }
  }

  @Inline
  private static int ceilDiv(int a, int b) {
    return (a + b - 1) / b;
  }

//  @Inline
//  public static Address tlabOf(final Address ptr) {
//    return ptr.toWord().and(TLAB_MASK.not()).toAddress();
//  }

  // Metadata setter
  @Inline
  public static Address of(final Address ptr) {
    return align(ptr);
  }

  @Inline
  public static Address of(final ObjectReference ref) {
    return of(VM.objectModel.refToAddress(ref));
  }

  @Inline
  public static void setRelocationState(Address region, boolean relocation) {
    metaDataOf(region, METADATA_RELOCATE_OFFSET).store((byte) (relocation ? 1 : 0));
  }

  @Inline
  public static boolean relocationRequired(Address region) {
    return metaDataOf(region, METADATA_RELOCATE_OFFSET).loadByte() != ((byte) 0);
  }

  @Inline
  public static void clearMarkBitMapForRegion(Address region) {
    Address chunk = EmbeddedMetaData.getMetaDataBase(region);
    int index = indexOf(region) + 1;
    Address start = chunk.plus(index * MARK_BYTES_PER_REGION);
    if (VM.VERIFY_ASSERTIONS) {
//      Log.writeln("start ", start);
      VM.assertions._assert(MARK_BYTES_PER_REGION * (REGIONS_IN_CHUNK + 1) == BYTES_IN_MARKTABLE);
      Address end = start.plus(MARK_BYTES_PER_REGION);
      VM.assertions._assert(end.LE(chunk.plus(BYTES_IN_MARKTABLE)));
    }
    VM.memory.zero(false, start, Extent.fromIntZeroExtend(MARK_BYTES_PER_REGION));
  }

  @Inline
  public static void setUsedSize(Address region, int bytes) {
    metaDataOf(region, METADATA_ALIVE_SIZE_OFFSET).store(bytes);
  }

  @Inline
  public static int usedSize(Address region) {
    int size = metaDataOf(region, METADATA_ALIVE_SIZE_OFFSET).loadInt();
    Address prevCursor = metaDataOf(region, METADATA_PREV_CURSOR_OFFSET).loadAddress();
    Address nextCursor = metaDataOf(region, METADATA_NEXT_CURSOR_OFFSET).loadAddress();
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(nextCursor.GE(prevCursor));
    int b = nextCursor.diff(prevCursor).toInt();
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(b >= 0);
    return size + b;
//    Address cursor = Region.metaDataOf(region, Region.METADATA_CURSOR_OFFSET).loadAddress();
//    Address tams = Region.metaDataOf(region, Region.METADATA_TAMS_OFFSET).loadAddress();
//    if (VM.VERIFY_ASSERTIONS) {
//      VM.assertions._assert(cursor.GE(region));
//      VM.assertions._assert(cursor.LE(region.plus(Region.BYTES_IN_REGION)));
//      VM.assertions._assert(tams.GE(region));
//      VM.assertions._assert(tams.LE(region.plus(Region.BYTES_IN_REGION)));
//      VM.assertions._assert(cursor.GE(tams));
//    }
//    int delta = cursor.diff(tams).toInt();
//    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(delta >= 0);
//    return size;// + delta;
  }

  @Inline
  public static int kind(Address region) {
    return Region.metaDataOf(region, Region.METADATA_GENERATION_OFFSET).loadInt();
  }

  @Inline
  public static void register(Address region, int allocationKind) {
    clearState(region);
    metaDataOf(region, METADATA_ALLOCATED_OFFSET).store((byte) 1);
    metaDataOf(region, METADATA_PREV_CURSOR_OFFSET).store(region);
    metaDataOf(region, METADATA_NEXT_CURSOR_OFFSET).store(region);
//    metaDataOf(region, METADATA_TAMS_OFFSET).store(region);
    if (VM.VERIFY_ASSERTIONS) {
      VM.assertions._assert(allocationKind >= 0 && allocationKind <= 2);
    }
    metaDataOf(region, METADATA_GENERATION_OFFSET).store(allocationKind);
  }

  @Inline
  public static void unregister(Address region) {
    clearState(region);
  }

  @Inline
  public static boolean allocated(Address region) {
    return metaDataOf(region, METADATA_ALLOCATED_OFFSET).loadByte() != ((byte) 0);
  }

  @Inline
  public static void updatePrevCursor(Address region) {
    Address nextCursor = metaDataOf(region, METADATA_NEXT_CURSOR_OFFSET).loadAddress();
    metaDataOf(region, METADATA_PREV_CURSOR_OFFSET).store(nextCursor);
  }

  @Inline
  public static boolean allocatedWithinConurrentMarking(ObjectReference o) {
    Address a = VM.objectModel.refToAddress(o);
    Address region = Region.of(a);
    Address prevCursor = metaDataOf(region, METADATA_PREV_CURSOR_OFFSET).loadAddress();
    Address nextCursor = metaDataOf(region, METADATA_NEXT_CURSOR_OFFSET).loadAddress();
    return prevCursor.LE(a) && a.LE(nextCursor);
  }

  @Inline
  public static void updateRegionAliveSize(Address region, ObjectReference object) {
    if (VM.VERIFY_ASSERTIONS) {
      VM.assertions._assert(VM.debugging.validRef(object));
    }
    Address meta = metaDataOf(region, METADATA_ALIVE_SIZE_OFFSET);
    int oldValue, size = VM.objectModel.getSizeWhenCopied(object);
    do {
      oldValue = meta.prepareInt();
    } while (!meta.attempt(oldValue, oldValue + size));
  }

  @Inline
  private static void clearState(Address region) {
    Address chunk = EmbeddedMetaData.getMetaDataBase(region);
    Address metaData = chunk.plus(PER_REGION_META_START_OFFSET);
    Address perRegionMeta = metaData.plus(PER_REGION_METADATA_BYTES * indexOf(region));
    if (VM.VERIFY_ASSERTIONS) {
//      VM.assertions._assert(perRegionMeta.GE(chunk.plus(BYTES_IN_PAGE * 16)));
//      VM.assertions._assert(perRegionMeta.LT(chunk.plus(BYTES_IN_PAGE * 17)));
    }
    VM.memory.zero(false, perRegionMeta, Extent.fromIntZeroExtend(PER_REGION_METADATA_BYTES));
  }

  @Inline
  private static Address align(final Address ptr) {
    return ptr.toWord().and(REGION_MASK.not()).toAddress();
  }

  @Inline
  public static boolean isAligned(Address region) {
    return region.toWord().and(REGION_MASK).isZero();
  }

  @Inline
  static int indexOf(Address region) {
    Address chunk = EmbeddedMetaData.getMetaDataBase(region);
    if (VM.VERIFY_ASSERTIONS) {
      if (region.EQ(chunk)) {
        Log.write("Invalid region ", region);
        Log.writeln(" chunk ", chunk);
      }
      VM.assertions._assert(region.NE(chunk));
    }
    int index = region.diff(chunk).toWord().rshl(LOG_BYTES_IN_REGION).toInt();
    if (VM.VERIFY_ASSERTIONS) {
      if (!(index >= 1 && index <= REGIONS_IN_CHUNK)) {
        Log.write("Invalid region ", region);
        Log.write(" chunk=", chunk);
        Log.write(" index=", index);
        Log.writeln(" region=", region);
      }
      VM.assertions._assert(index >= 1 && index <= REGIONS_IN_CHUNK);
    }
    return index - 1;
  }

  @Inline
  public static Address metaDataOf(Address region, int metaDataOffset) {
    Address chunk = EmbeddedMetaData.getMetaDataBase(region);
    Address perRegionMetaData = chunk.plus(PER_REGION_META_START_OFFSET);
    Address meta = perRegionMetaData.plus(PER_REGION_METADATA_BYTES * indexOf(region)).plus(metaDataOffset);
    if (VM.VERIFY_ASSERTIONS) {
//      VM.assertions._assert(meta.GE(chunk.plus(BYTES_IN_PAGE * 16)));
//      VM.assertions._assert(meta.LT(chunk.plus(BYTES_IN_PAGE * 17)));
    }
    return meta;
  }

  @Inline
  public static Address allocate(Address region, int size, boolean atomic) {
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(isAligned(region));
    Address slot = metaDataOf(region, METADATA_NEXT_CURSOR_OFFSET);
    Address regionEnd = region.plus(BYTES_IN_REGION);
    if (atomic) {
      Address oldValue, newValue;
      do {
        oldValue = slot.prepareAddress();
        newValue = oldValue.plus(size);
        if (newValue.GT(regionEnd)) return Address.zero();
      } while (!slot.attempt(oldValue, newValue));
      return oldValue;
    } else {
      Address oldValue = slot.loadAddress();
      Address newValue = oldValue.plus(size);
      if (newValue.GT(regionEnd)) return Address.zero();
      slot.store(newValue);
      return oldValue;
    }
  }
}
