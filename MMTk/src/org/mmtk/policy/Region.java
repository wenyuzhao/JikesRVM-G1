package org.mmtk.policy;


import org.mmtk.utility.Constants;
import org.mmtk.utility.Log;
import org.mmtk.utility.alloc.BlockAllocator;
import org.mmtk.utility.alloc.EmbeddedMetaData;
import org.mmtk.utility.alloc.LinearScan;
import org.mmtk.utility.options.Options;
import org.mmtk.vm.Lock;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.NoInline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.*;

//import static org.mmtk.policy.Region.Card.OBJECT_END_ADDRESS_OFFSET;
import static org.mmtk.utility.Constants.*;

@Uninterruptible
public class Region {
//  public static final boolean VERBOSE;
  @Inline
  public static final boolean verbose() {
    return VM.VERIFY_ASSERTIONS && Options.verbose.getValue() != 0;
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
  public static final int METADATA_CURSOR_OFFSET = METADATA_ALLOCATED_OFFSET + BYTES_IN_SHORT;//BYTES_IN_BYTE;
  public static final int METADATA_REMSET_LOCK_OFFSET = METADATA_CURSOR_OFFSET + BYTES_IN_ADDRESS;
  public static final int METADATA_REMSET_SIZE_OFFSET = METADATA_REMSET_LOCK_OFFSET + BYTES_IN_INT;
  public static final int METADATA_REMSET_PAGES_OFFSET = METADATA_REMSET_SIZE_OFFSET + BYTES_IN_INT;
  public static final int METADATA_REMSET_POINTER_OFFSET = METADATA_REMSET_PAGES_OFFSET + BYTES_IN_INT;
  public static final int METADATA_GENERATION_OFFSET = METADATA_REMSET_POINTER_OFFSET + BYTES_IN_ADDRESS;
  public static final int METADATA_TAMS_OFFSET = METADATA_GENERATION_OFFSET + BYTES_IN_ADDRESS;
  private static final int PER_REGION_METADATA_BYTES = METADATA_TAMS_OFFSET + BYTES_IN_ADDRESS;
  private static final int PER_REGION_META_START_OFFSET = BYTES_IN_MARKTABLE;

  public static final int METADATA_PAGES_PER_CHUNK = (1 << LOG_PAGES_IN_MARKTABLE)  + 1;

  static {
    if (VM.VERIFY_ASSERTIONS) {
      VM.assertions._assert(LOG_PAGES_IN_REGION >= 4 && LOG_PAGES_IN_REGION <= 8);
      VM.assertions._assert(PER_REGION_METADATA_BYTES == 36);
//        VM.assertions._assert(REGIONS_IN_CHUNK == 3);
    }
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
    Address cursor = Region.metaDataOf(region, Region.METADATA_CURSOR_OFFSET).loadAddress();
    Address tams = Region.metaDataOf(region, Region.METADATA_TAMS_OFFSET).loadAddress();
    if (VM.VERIFY_ASSERTIONS) {
      VM.assertions._assert(cursor.GE(region));
      VM.assertions._assert(cursor.LE(region.plus(Region.BYTES_IN_REGION)));
      VM.assertions._assert(tams.GE(region));
      VM.assertions._assert(tams.LE(region.plus(Region.BYTES_IN_REGION)));
      VM.assertions._assert(cursor.GE(tams));
    }
    int delta = cursor.diff(tams).toInt();
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(delta >= 0);
    return size + delta;
  }

  @Inline
  public static int kind(Address region) {
    return Region.metaDataOf(region, Region.METADATA_GENERATION_OFFSET).loadInt();
  }

  @Inline
  public static void register(Address region, int allocationKind) {
    clearState(region);
    metaDataOf(region, METADATA_ALLOCATED_OFFSET).store((byte) 1);
    metaDataOf(region, METADATA_CURSOR_OFFSET).store(region);
    metaDataOf(region, METADATA_TAMS_OFFSET).store(region);
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
  public static void updateRegionAliveSizeNonAtomic(Address region, int size) {
    Address meta = metaDataOf(region, METADATA_ALIVE_SIZE_OFFSET);
    meta.store(meta.loadInt() + size);
  }

  @Inline
  public static Address getCursor(Address region) {
    return metaDataOf(region, METADATA_CURSOR_OFFSET).loadAddress();
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

  public static boolean USE_CARDS = VM.activePlan.constraints().G1_CARD_SUPPORT();

  @Uninterruptible
  public static class Card {
    public static final int LOG_BYTES_IN_CARD = 9;
    public static final int BYTES_IN_CARD = 1 << LOG_BYTES_IN_CARD;
    public static final Word CARD_MASK = Word.fromIntZeroExtend(BYTES_IN_CARD - 1);// 0..0111111111
    public static int[] anchors;
    public static int[] limits;

    static {
      if (USE_CARDS) {
        int memorySize = VM.HEAP_END.diff(VM.HEAP_START).toInt();
        int totalCards = memorySize >>> Card.LOG_BYTES_IN_CARD;
        int entries = totalCards >>> 2;
        anchors = new int[entries];
        limits = new int[entries];

        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(((byte) 0xFF) == ((byte) -1));

        for (int i = 0; i < entries; i++) {
          anchors[i] = 0xFFFFFFFF;
          limits[i] = 0xFFFFFFFF;
        }
      }
    }

    @Inline
    public static boolean compareAndSwapByteInBuffer(int[] buf, int index, byte oldByte, byte newByte) {
//      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(oldByte != newByte);
      int intIndex = index >>> 2;
      int byteIndex = index ^ (intIndex << 2);
//      if (VM.VERIFY_ASSERTIONS) {
//        VM.assertions._assert(intIndex == index / 4 && byteIndex == index % 4);
//      }
      Offset offset = Offset.fromIntZeroExtend(intIndex << 2);
      // Get old int
      int oldValue = buf[intIndex];
      // Build new int
      int newValue = oldValue & ~(0xff << ((3 - byteIndex) << 3)); // Drop the target byte
      newValue |= ((((int) newByte) & 0xFF) << ((3 - byteIndex) << 3)); // Set new byte

//      if (VM.VERIFY_ASSERTIONS) {
//        if (byteIndex == 0) VM.assertions._assert((newValue << 8) == (oldValue << 8));
//        if (byteIndex == 1) VM.assertions._assert((newValue << 16) == (oldValue << 16) && (newValue >>> 24) == (oldValue >>> 24));
//        if (byteIndex == 2) VM.assertions._assert((newValue << 24) == (oldValue << 24) && (newValue >>> 16) == (oldValue >>> 16));
//        if (byteIndex == 3) VM.assertions._assert((newValue >>> 8) == (oldValue >>> 8));
//      }
      //if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(oldValue != newValue);
      if (oldValue == newValue) return false;
      return VM.objectModel.attemptInt(buf, offset, oldValue, newValue);
    }

    @Inline
    public static byte getByte(int[] buf, int index) {
      int intIndex = index >>> 2;
      int byteIndex = index ^ (intIndex << 2);
      int entry = buf[intIndex];
      return (byte) ((entry << (byteIndex << 3)) >>> 24);
    }

    @Inline
    public static boolean isAligned(Address card) {
      return card.toWord().and(CARD_MASK.not()).toAddress().EQ(card);
    }

    @Inline
    public static Address of(Address address) {
      return address.toWord().and(CARD_MASK.not()).toAddress();
    }

    @Inline
    public static Address of(ObjectReference ref) {
//      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!ref.isNull());
      return of(VM.objectModel.objectStartRef(ref));
    }

    @Inline
    public static int indexOf(Address card) {
      Address region = Region.of(card);
      int index = card.diff(region).toInt() >>> LOG_BYTES_IN_CARD;
      return index;
    }

    @Inline
    public static int hash(Address card) {
      return card.diff(VM.HEAP_START).toInt() >>> Card.LOG_BYTES_IN_CARD;
    }

    @Inline
    public static void updateCardMeta(ObjectReference ref) {
      // set anchor value
      final Address objectStartAddress = VM.objectModel.objectStartRef(ref);
      final Address card = Card.of(objectStartAddress);
      final int cardIndex = hash(card);
//      if ((cardIndex >>> 2) >= anchors.length) {
//        Log.write("Overflow cardIndex=", cardIndex);
//        Log.write(" card=", card);
//        Log.write(" ref=", ref);
//        Log.writeln(" length=", anchors.length);
//      }
      // CAS anchor value
      byte oldStartOffset, newStartOffset;
      do {
        // Get old value
        oldStartOffset = getByte(anchors, cardIndex);
        // Build new value
        newStartOffset = (byte) (objectStartAddress.diff(card).toInt() >>> Constants.LOG_BYTES_IN_ADDRESS);
        // Break if (old != -1 && old <= new)
        if (oldStartOffset != ((byte) -1) && oldStartOffset <= newStartOffset) break;
      } while (!compareAndSwapByteInBuffer(anchors, cardIndex, oldStartOffset, newStartOffset));

      // set limit value
      final Address objectEndAddress = VM.objectModel.getObjectEndAddress(ref);
      final Address endCard = Card.of(objectEndAddress);

      // CAS limit value
      byte oldEndOffset, newEndOffset;
      do {
        // Get old value
        oldEndOffset = getByte(limits, cardIndex);
        // Build new value
        if (endCard.EQ(card)) {
          newEndOffset = (byte) (objectEndAddress.diff(card).toInt() >>> Constants.LOG_BYTES_IN_ADDRESS);
        } else {
          newEndOffset = (byte) 0;
        }
        // Break if old >= new or oldEndOffset is already 0
        if (oldEndOffset == 0) break;
        if (newEndOffset != 0 && oldEndOffset >= newEndOffset) break;
      } while (!compareAndSwapByteInBuffer(limits, cardIndex, oldEndOffset, newEndOffset));
    }

    @Inline
    public static Address getCardAnchor(Address card) {
      //if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(card.EQ(Card.of(card)));
      int cardIndex = hash(card);
      byte offset = getByte(anchors, cardIndex);
      if (offset == (byte) -1) return Address.zero();
      return card.plus(offset << Constants.LOG_BYTES_IN_ADDRESS);
    }

    @Inline
    public static Address getCardLimit(Address card) {
      //if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(card.EQ(Card.of(card)));
      int cardIndex = hash(card);
      byte offset = getByte(limits, cardIndex);
      if (offset == (byte) -1) return Address.zero(); // limit is unset
      return card.plus(offset == 0 ? BYTES_IN_CARD : (offset << Constants.LOG_BYTES_IN_ADDRESS));
    }

    @Inline
    public static void clearCardMeta(Address card) {
      int i = hash(card);
      ObjectReference.fromObject(anchors).toAddress().plus(i).store((byte) -1);
      ObjectReference.fromObject(limits).toAddress().plus(i).store((byte) -1);
    }

    @Inline
    public static void clearCardMetaForRegion(Address region) {
      Address end = region.plus(Region.BYTES_IN_REGION);
      for (Address c = region; c.LT(end); c = c.plus(Card.BYTES_IN_CARD)) {
        clearCardMeta(c);
      }
    }

    @Inline
    public static void clearCardMetaForUnmarkedCards(RegionSpace regionSpace, boolean concurrent, boolean nursery) {
      int workers = VM.activePlan.collector().parallelWorkerCount();
      int id = VM.activePlan.collector().getId();
      if (concurrent) id -= workers;
      int totalEntries = anchors.length;
      int entriesToClear = ceilDiv(anchors.length, workers);
      Address anchorsPtr = ObjectReference.fromObject(anchors).toAddress();
      Address limitsPtr = ObjectReference.fromObject(limits).toAddress();

      int G1_SPACE = regionSpace.getDescriptor();

      for (int i = 0; i < entriesToClear; i++) {
        int index = entriesToClear * id + i;
        if (index >= totalEntries) break;
        int firstCardIndex = index << Constants.LOG_BYTES_IN_INT;
        Address firstCard = VM.HEAP_START.plus(firstCardIndex << LOG_BYTES_IN_CARD);
        if (!Space.isMappedAddress(firstCard)) {
          anchors[index] = 0xFFFFFFFF;
          limits[index] = 0xFFFFFFFF;
          continue;
        }
        if (Space.isInSpace(G1_SPACE, firstCard)) {
          Address region = Region.of(firstCard);
          if (region.EQ(EmbeddedMetaData.getMetaDataBase(firstCard))) continue;
          if (!Region.allocated(region)) {
            anchors[index] = 0xFFFFFFFF;
            limits[index] = 0xFFFFFFFF;
          }
          continue;
        }
        if (nursery) continue;
//        if (Space.isInSpace(G1_SPACE, firstCard)) continue;

        Space space = Space.getSpaceForAddress(firstCard);
        if (true) {
          if (space instanceof SegregatedFreeListSpace) {
            if (!BlockAllocator.checkBlockMeta(firstCard)) {
              anchors[index] = 0xFFFFFFFF;
              limits[index] = 0xFFFFFFFF;
            }
          } else if (space instanceof LargeObjectSpace) {
//            if (((LargeObjectSpace) space).isInNurseryOrFromSpace(firstCard)) {
//              anchors[index] = 0xFFFFFFFF;
//              limits[index] = 0xFFFFFFFF;
//            }
          } else if (space instanceof RawPageSpace) {
            anchors[index] = 0xFFFFFFFF;
            limits[index] = 0xFFFFFFFF;
          }
        } else {
          if (space instanceof SegregatedFreeListSpace) {
            if (!BlockAllocator.checkBlockMeta(firstCard)) {
              anchors[index] = 0xFFFFFFFF;
              limits[index] = 0xFFFFFFFF;
            }
          }
          if (space instanceof RawPageSpace) {
            anchors[index] = 0xFFFFFFFF;
            limits[index] = 0xFFFFFFFF;
          }
        }
//          if (Space.isInSpace(G1_SPACE, firstCard)) continue;
//          anchors[index] = 0xFFFFFFFF;
//          limits[index] = 0xFFFFFFFF;
      }
    }
    public static String tag = null;

//    public static final Offset OBJECT_END_ADDRESS_OFFSET = VM.objectModel.GC_HEADER_OFFSET().plus(Constants.BYTES_IN_ADDRESS);

    @Inline
    @Uninterruptible
    public static void linearScan(LinearScan scan, RegionSpace regionSpace, Address card, boolean verbose) {
      Address end = getCardLimit(card);
      if (end.isZero()) return;
      if (VM.VERIFY_ASSERTIONS) {
        VM.assertions._assert(!end.isZero());
        VM.assertions._assert(end.GE(card));
        VM.assertions._assert(end.LE(card.plus(BYTES_IN_CARD)));
      }
      final Address cursor = Region.Card.getCardAnchor(card);
      if (cursor.isZero()) return;
      if (VM.VERIFY_ASSERTIONS) {
        VM.assertions._assert(!cursor.isZero());
        VM.assertions._assert(cursor.GE(card));
        VM.assertions._assert(cursor.LT(end));
      }
      final Space cardSpace = Space.getSpaceForAddress(card);
      final boolean inMSSpace = cardSpace instanceof MarkSweepSpace;
      final boolean inRegionSpace = cardSpace instanceof RegionSpace;

      if (Region.verbose() && verbose) {
        Log log = VM.activePlan.mutator().getLog();
        log.write("scan card ", card);
        log.write(": ", cursor);
        log.write("..", end);
        log.write(" ");
        log.writeln(Space.getSpaceForAddress(card).getName());
      }

      Address regionEnd = Address.zero();
      if (inRegionSpace) {
        regionEnd = Region.of(card).plus(BYTES_IN_REGION);
        if (VM.VERIFY_ASSERTIONS) {
          VM.assertions._assert(Region.of(card).NE(EmbeddedMetaData.getMetaDataBase(card)));
        }
//        if (regionEnd.)
      }

      final int objectRefOffset = VM.objectModel.getObjectRefOffset();

      ObjectReference ref = inRegionSpace ? getObjectFromStartAddress(cursor, end, objectRefOffset) : VM.objectModel.getObjectFromStartAddress(cursor);
      if (ref.isNull() || VM.objectModel.objectStartRef(ref).GE(end)) return;//VM.objectModel.getObjectFromStartAddress(cursor);

      do {
        // Get next object start address, i.e. current object end address
        Address currentObjectEnd;
        if (inMSSpace) {
          // VM.objectModel.dumpObject(ref);
          MarkSweepSpace space = (MarkSweepSpace) Space.getSpaceForAddress(card);
          // Get current block
          Address block = BlockAllocator.getBlkStart(ref.toAddress());
          // Get cell size classes
          byte cellSizeClass = BlockAllocator.getClientSizeClass(VM.objectModel.objectStartRef(ref));
          // Get first cell address
//          Address firstCell = block.plus(space.getBlockHeaderSize(cellSizeClass));//BlockAllocator.getFreeListMeta(block);
          // Get cell extent
          int cellExtent = space.getBaseCellSize(cellSizeClass);
          // Get current cell for `ref`
//          int cellIndex = VM.objectModel.objectStartRef(ref).diff(firstCell).toInt() / cellExtent;
//          Address currentCell = firstCell.plus(cellExtent * cellIndex);
          // Get next freelist start address
//          Address nextCell = currentCell.plus(cellExtent);
          //
//          currentObjectEnd = nextCell;//.plus(Constants.BYTES_IN_ADDRESS);
        } else {
            currentObjectEnd = VM.objectModel.getObjectEndAddress(ref);
        }

//        if (Region.verbose() && verbose) {
//          Log.writeln("scan ", ref);
//        }
        scan.scan(ref);

//        if (currentObjectEnd.GE(end)) {
//          break;
//        } else {
//          ObjectReference next = inRegionSpace ? getObjectFromStartAddress(currentObjectEnd, end, objectRefOffset) : VM.objectModel.getObjectFromStartAddress(currentObjectEnd);
//          if (next.isNull() || VM.objectModel.objectStartRef(next).GE(end)) break;
//          ref = next;
//        }
      } while (true);
    }
  }

  @Inline
  public static void linearScan(LinearScan scan, final Address region) {
    if (VM.VERIFY_ASSERTIONS) {
      VM.assertions._assert(!region.isZero());
      VM.assertions._assert(relocationRequired(region));
    }
//    final Address end = getCursor(region);
    final Address limit = metaDataOf(region, METADATA_CURSOR_OFFSET).loadAddress();//region.plus(BYTES_IN_REGION);
    if (VM.VERIFY_ASSERTIONS) {
//      VM.assertions._assert(!end.isZero());
//      VM.assertions._assert(end.LE(limit));
    }
    if (verbose()) {
      Log.write("Evacuate region ", region);
      Log.writeln(" ~ ", limit);
    }

    Address cursor = region;
//    if (cursor.GE(end)) {
//      return;
//    }
    final int objectRefOffset = VM.objectModel.getObjectRefOffset();

    ObjectReference ref;
    while (!(ref = getObjectFromStartAddress(cursor, limit, objectRefOffset)).isNull()) {
      if (VM.VERIFY_ASSERTIONS) {
        VM.assertions._assert(VM.debugging.validRef(ref));
        VM.assertions._assert(Region.of(ref).EQ(region));
      }
//      if (!ref.toAddress().loadWord(OBJECT_END_ADDRESS_OFFSET).isZero()) {
//        cursor = ref.toAddress().loadWord(OBJECT_END_ADDRESS_OFFSET).toAddress();
//      } else {
//        VM.objectModel.dumpObject(ref);

        cursor = VM.objectModel.getObjectEndAddress(ref);
//        Log.writeln("End ", cursor);
        scan.scan(ref);
//      }
//      cursor = VM.objectModel.getObjectEndAddress(ref);
    }

//    ObjectReference ref = VM.objectModel.getObjectFromStartAddress(cursor);
//    do {
//      Address currentObjectEnd;
//      if (VM.VERIFY_ASSERTIONS) {
//        VM.assertions._assert(VM.debugging.validRef(ref));
//        VM.assertions._assert(Region.of(ref).EQ(region));
//      }
//      currentObjectEnd = VM.objectModel.getObjectEndAddress(ref);
//      scan.scan(ref);
//      if (currentObjectEnd.GT(limit)) {
//        break;
//      }
//      ref = VM.objectModel.getObjectFromStartAddress(currentObjectEnd);
//      if (VM.objectModel.objectStartRef(ref).GE(limit)) break;
//      if (VM.objectModel.refToAddress(ref).loadInt() == 0) break;
//    } while (true);
  }

  @Inline
  private static ObjectReference getObjectFromStartAddress(Address start, Address limit, int objectRefOffset) {
    if (VM.VERIFY_ASSERTIONS) {
      VM.assertions._assert(!start.isZero());
      VM.assertions._assert(!limit.isZero());
//      VM.assertions._assert(start.LT(regionEnd));
    }
//    while (true) {
      if (start.GE(limit)) return ObjectReference.nullReference();
      while ((start.loadInt()) == ALIGNMENT_VALUE) {
        start = start.plus(BYTES_IN_INT);
        if (start.GE(limit)) return ObjectReference.nullReference();
      }
      ObjectReference ref = start.plus(objectRefOffset).toObjectReference();
//      VM.objectModel.dumpObject(ref);
      if (VM.objectModel.objectStartRef(ref).GE(limit)) {
        return ObjectReference.nullReference();
      } else {
        int v = VM.objectModel.refToAddress(ref).loadInt();
        if (v == 0) {
          return ObjectReference.nullReference();
//          start = tlabOf(start).plus(BYTES_IN_TLAB);
        } else {
          if (VM.VERIFY_ASSERTIONS) {
            VM.assertions._assert(v != VM.ALIGNMENT_VALUE);
            VM.assertions._assert(!ref.isNull());
//            if (ref.toAddress().loadWord(OBJECT_END_ADDRESS_OFFSET).isZero()) {
              VM.assertions._assert(!VM.objectModel.refToAddress(ref).loadObjectReference().isNull());
              VM.assertions._assert(VM.debugging.validRef(ref));
//            }
          }
          return ref;
        }
      }
//    }
//    while (true) {
//      if (start.GE(regionEnd)) return ObjectReference.nullReference();
//      final int v = start.loadInt();
//      if (v == 0 || v == VM.ALIGNMENT_VALUE) {
//        start = start.plus(BYTES_IN_INT);
//      } else {
//        break;
//      }
//    }
//    return VM.objectModel.getObjectFromStartAddress(start);
  }
}
