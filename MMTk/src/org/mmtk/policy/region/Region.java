package org.mmtk.policy.region;


//import org.mmtk.policy.*;
import org.mmtk.plan.Plan;
import org.mmtk.plan.g1.G1;
import org.mmtk.utility.Log;
import org.mmtk.utility.alloc.EmbeddedMetaData;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.*;

//import static org.mmtk.policy.region.Region.Card.OBJECT_END_ADDRESS_OFFSET;
import static org.mmtk.utility.Constants.*;

@Uninterruptible
public class Region {
  public static final boolean VERBOSE_REGION_LIFETIME = false;

  // Generation information
  public static final int EDEN = 0;
  public static final int SURVIVOR = 1;
  public static final int OLD = 2;

  // Region size
//  public static final boolean USE_PLAN_SPECIFIC_REGION_SIZE = true;

  public static final int LOG_PAGES_IN_REGION = VM.activePlan.constraints().g1LogPagesInRegion();
  public static final int PAGES_IN_REGION = 1 << LOG_PAGES_IN_REGION;
  public static final int LOG_BYTES_IN_REGION = LOG_PAGES_IN_REGION + LOG_BYTES_IN_PAGE;
  public static final int BYTES_IN_REGION = 1 << LOG_BYTES_IN_REGION;
  public static final int MAX_ALLOC_SIZE = (int) (BYTES_IN_REGION / 2);
  private static final int REGIONS_IN_CHUNK = (1 << (EmbeddedMetaData.LOG_PAGES_IN_REGION - LOG_PAGES_IN_REGION));
  public static final Word REGION_MASK = Word.fromIntZeroExtend(BYTES_IN_REGION - 1);// 0..011111111111

  // Mark table:
  // 1 bit per 4 byte: 1/32 ratio
  // 4M MMTk block ~> 128kb (32 pages)

  // Metadata layout:
  // Total 17 pages:
  // Page 0-15: mark table
  // Page 16: Per region metadata

  // Mark table
  private static final int LOG_PAGES_IN_MARKTABLE = EmbeddedMetaData.LOG_PAGES_IN_REGION - 5;
  public static final int BYTES_IN_MARKTABLE = 1 << (LOG_PAGES_IN_MARKTABLE + LOG_BYTES_IN_PAGE);
  public static final int MARKTABLE0_OFFSET = 0;
  public static final int MARKTABLE1_OFFSET = BYTES_IN_MARKTABLE;
  public static final int MARK_BYTES_PER_REGION = BYTES_IN_MARKTABLE / REGIONS_IN_CHUNK;
  public static final int MARK_PAGES_PER_CHUNK = (1 << LOG_PAGES_IN_MARKTABLE) * 2;
  // Per region metadata (offsets)
  public static final int MD_LIVE_SIZE = 0;
  public static final int MD_RELOCATE = MD_LIVE_SIZE + BYTES_IN_INT;
  public static final int MD_ALLOCATED = MD_RELOCATE + BYTES_IN_SHORT;//BYTES_IN_BYTE;
  public static final int MD_ACTIVE_MARKTABLE = MD_ALLOCATED + BYTES_IN_SHORT;
  public static final int MD_PREV_CURSOR = MD_ACTIVE_MARKTABLE + BYTES_IN_INT;//BYTES_IN_BYTE;
  public static final int MD_NEXT_CURSOR = MD_PREV_CURSOR + BYTES_IN_ADDRESS;//BYTES_IN_BYTE;
  public static final int MD_REMSET_LOCK = MD_NEXT_CURSOR + BYTES_IN_ADDRESS;
  public static final int MD_REMSET_SIZE = MD_REMSET_LOCK + BYTES_IN_INT;
  public static final int MD_REMSET_PAGES = MD_REMSET_SIZE + BYTES_IN_INT;
  public static final int MD_REMSET = MD_REMSET_PAGES + BYTES_IN_INT;
  public static final int MD_REMSET_HEAD_PRT = MD_REMSET + BYTES_IN_ADDRESS;
  public static final int MD_CARD_OFFSET_TABLE = MD_REMSET_HEAD_PRT + BYTES_IN_ADDRESS;
  public static final int MD_GENERATION = MD_CARD_OFFSET_TABLE + BYTES_IN_ADDRESS;
  public static final int MD_NEXT_REGION = MD_GENERATION + BYTES_IN_INT;

  private static final int PER_REGION_METADATA_BYTES = MD_NEXT_REGION + BYTES_IN_ADDRESS;
  private static final int PER_REGION_META_START_OFFSET = BYTES_IN_MARKTABLE << 1;

//  public static final int METADATA_PAGES_PER_CHUNK = (1 << LOG_PAGES_IN_MARKTABLE) + 1;//(META_REGIONS_PER_CHUNK * PAGES_IN_REGION);
  private static final int METADATA_BYTES = MARK_PAGES_PER_CHUNK * BYTES_IN_PAGE + PER_REGION_METADATA_BYTES * REGIONS_IN_CHUNK;

//  public static final int METADATA_PAGES_PER_CHUNK = MARK_PAGES_PER_CHUNK + 1;
  private static final int META_REGIONS_PER_CHUNK = (METADATA_BYTES + BYTES_IN_REGION - 1) / BYTES_IN_REGION;
  public static final int METADATA_PAGES_PER_CHUNK = META_REGIONS_PER_CHUNK * PAGES_IN_REGION;
  public static final float MEMORY_RATIO = ((float) (REGIONS_IN_CHUNK - META_REGIONS_PER_CHUNK)) / ((float) REGIONS_IN_CHUNK); // 0.75

  static {
    int metadataBytes = (1 << LOG_PAGES_IN_MARKTABLE) * BYTES_IN_PAGE * 2;
    metadataBytes += PER_REGION_METADATA_BYTES * REGIONS_IN_CHUNK;
    if (metadataBytes > META_REGIONS_PER_CHUNK * BYTES_IN_REGION) {
      Log.writeln("META_REGIONS_PER_CHUNK=", META_REGIONS_PER_CHUNK);
      Log.writeln("metadataBytes=", metadataBytes);
      VM.assertions.fail("Region size too small");
    }
//    if (VM.VERIFY_ASSERTIONS) {
//      VM.assertions._assert(LOG_PAGES_IN_REGION >= 4 && LOG_PAGES_IN_REGION <= 8);
//      VM.assertions._assert(PER_REGION_METADATA_BYTES == 40);
//    }
  }

  @Inline
  private static int ceilDiv(int a, int b) {
    return (a + b - 1) / b;
  }

  @Inline
  public static void set(Address region, int offset, Address value) {
    metaDataOf(region, offset).store(value);
  }
  @Inline
  public static void set(Address region, int offset, boolean value) {
    metaDataOf(region, offset).store(value ? (byte) 1 : (byte) 0);
  }
  @Inline
  public static void set(Address region, int offset, int value) {
    metaDataOf(region, offset).store(value);
  }

  @Inline
  public static Address getAddress(Address region, int offset) {
    return metaDataOf(region, offset).loadAddress();
  }
  @Inline
  public static int getInt(Address region, int offset) {
    return metaDataOf(region, offset).loadInt();
  }
  @Inline
  public static boolean getBool(Address region, int offset) {
    return metaDataOf(region, offset).loadByte() != (byte) 0;
  }
  @Inline
  public static Address metaSlot(Address region, int offset) {
    return metaDataOf(region, offset);
  }

  @Inline
  public static Address of(final Address ptr) {
    return align(ptr);
  }

  @Inline
  public static Address of(final ObjectReference ref) {
    return of(VM.objectModel.refToAddress(ref));
  }

  @Inline
  public static Address getNext(Address prev) {
    return getAddress(prev, MD_NEXT_REGION);
  }

  @Inline
  public static int liveBytes(Address region) {
    int a = metaDataOf(region, MD_LIVE_SIZE).loadInt();
    Address prevCursor = metaDataOf(region, MD_PREV_CURSOR).loadAddress();
    Address nextCursor = metaDataOf(region, MD_NEXT_CURSOR).loadAddress();
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(nextCursor.GE(prevCursor));
    int b = nextCursor.diff(prevCursor).toInt();
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(b >= 0);
    return a + b;
  }

  @Inline
  public static void register(Address region, int allocationKind) {
//    MarkTable.clearAllTables(region);
    clearState(region);
    set(region, MD_ALLOCATED, true);
    set(region, MD_PREV_CURSOR, region);
    set(region, MD_NEXT_CURSOR, region);
    if (G1.ENABLE_REMEMBERED_SETS) {
      set(region, MD_REMSET, Plan.metaDataSpace.acquire(RemSet.PAGES_IN_REMSET));
      set(region, MD_CARD_OFFSET_TABLE, Plan.metaDataSpace.acquire(CardOffsetTable.PAGES_IN_CARD_OFFSET_TABLE));
    }
    if (VM.VERIFY_ASSERTIONS) {
      VM.assertions._assert(allocationKind >= 0 && allocationKind <= 2);
    }
    set(region, MD_GENERATION, allocationKind);
//    if (allocationKind != OLD) {
//      CardTable.setAllNursery(region);
//    }
  }

  @Inline
  public static void unregister(Address region) {
    if (G1.ENABLE_REMEMBERED_SETS) {
      Address remset = getAddress(region, MD_REMSET);
      Address headPRT = getAddress(region, MD_REMSET_HEAD_PRT);
      RemSet.releasePRTs(headPRT);
      Plan.metaDataSpace.release(remset);
      Plan.metaDataSpace.release(getAddress(region, MD_CARD_OFFSET_TABLE));
    }
    clearState(region);
  }

  @Inline
  public static void updatePrevCursor(Address region) {
    Address nextCursor = metaDataOf(region, MD_NEXT_CURSOR).loadAddress();
    metaDataOf(region, MD_PREV_CURSOR).store(nextCursor);
  }

  @Inline
  public static boolean allocatedWithinConurrentMarking(ObjectReference o) {
    Address a = VM.objectModel.objectStartRef(o);
    Address prevCursor = getAddress(Region.of(a), MD_PREV_CURSOR);
    return a.GE(prevCursor);
  }

  @Inline
  public static void updateRegionAliveSize(Address region, ObjectReference object) {
    if (VM.VERIFY_ASSERTIONS) {
      VM.assertions._assert(VM.debugging.validRef(object));
    }
    Address meta = metaDataOf(region, MD_LIVE_SIZE);
    int oldValue, size = VM.objectModel.getSizeWhenCopied(object);
    do {
      oldValue = meta.prepareInt();
    } while (!meta.attempt(oldValue, oldValue + size));
  }

  @Inline
  private static void clearState(Address region) {
    Address chunk = EmbeddedMetaData.getMetaDataBase(region);
    Address metaData = chunk.plus(PER_REGION_META_START_OFFSET);
    Address perRegionMeta = metaData.plus(PER_REGION_METADATA_BYTES * indexOf2(region));
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

  /// Returns the region's logical index within the 4MB chunk
  /// Guarantee 1 <= index <= 2 (for 1MB region)
  @Inline
  static int indexOf2(Address region) {
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
      if (!(index >= 1 && index < REGIONS_IN_CHUNK)) {
        Log.write("Invalid region ", region);
        Log.write(" chunk=", chunk);
        Log.write(" index=", index);
        Log.writeln(" region=", region);
      }
      VM.assertions._assert(index >= 1 && index < REGIONS_IN_CHUNK);
    }
    return index;
  }

  @Inline
  private static Address metaDataOf(Address region, int metaDataOffset) {
    Address chunk = EmbeddedMetaData.getMetaDataBase(region);
    Address perRegionMetaData = chunk.plus(PER_REGION_META_START_OFFSET);
    Address meta = perRegionMetaData.plus(PER_REGION_METADATA_BYTES * indexOf2(region)).plus(metaDataOffset);
    if (VM.VERIFY_ASSERTIONS) {
//      VM.assertions._assert(meta.GE(chunk.plus(BYTES_IN_PAGE * 16)));
//      VM.assertions._assert(meta.LT(chunk.plus(BYTES_IN_PAGE * 17)));
    }
    return meta;
  }

  @Inline
  public static Address allocate(final Address region, final int size, final boolean atomic) {
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(isAligned(region));
    Address slot = metaDataOf(region, MD_NEXT_CURSOR);
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

  @Inline
  public static int heapIndexOf(Address region) {
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!region.isZero() && isAligned(region));
    return region.diff(VM.HEAP_START).toWord().rshl(LOG_BYTES_IN_REGION).toInt();
  }

  static String getGenerationName(Address region) {
    return getGenerationName(getInt(region, MD_GENERATION));
  }
  static String getGenerationName(int gen) {
    switch (gen) {
      case EDEN:     return "eden";
      case SURVIVOR: return "survivor";
      case OLD:      return "old";
      default:       return "???";
    }
  }
}
