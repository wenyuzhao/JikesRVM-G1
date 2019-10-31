package org.mmtk.policy.region;

import org.mmtk.utility.alloc.EmbeddedMetaData;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.*;

import static org.mmtk.utility.Constants.*;

@Uninterruptible
public class MarkTable {
//  static final Word CHUNK_MASK = Word.fromIntZeroExtend(EmbeddedMetaData.BYTES_IN_REGION - 1);
//
//  @Inline
//  public static boolean isMarked(ObjectReference object) {
//    Address addr = VM.objectModel.refToAddress(object);
//    int bitIndex = addr.toWord().and(CHUNK_MASK).rshl(LOG_BYTES_IN_WORD).toInt();
//    Address markWord = EmbeddedMetaData.getMetaDataBase(addr).plus(bitIndex >>> LOG_BITS_IN_WORD);
//    if (VM.VERIFY_ASSERTIONS) {
//      Address chunk = EmbeddedMetaData.getMetaDataBase(addr);
//      VM.assertions._assert(markWord.GE(chunk));
//      VM.assertions._assert(markWord.LT(chunk.plus(Region.BYTES_IN_MARKTABLE)));
//    }
//    Word mask = Word.one().lsh(bitIndex & (BITS_IN_WORD - 1));
//    return !markWord.loadWord().and(mask).isZero();
//  }
//
//  @Inline
//  public static boolean testAndMark(ObjectReference object) {
//    Address addr = VM.objectModel.refToAddress(object);
//    int bitIndex = addr.toWord().and(CHUNK_MASK).rshl(LOG_BYTES_IN_WORD).toInt();
//    Address markWord = EmbeddedMetaData.getMetaDataBase(addr).plus(bitIndex >>> LOG_BITS_IN_WORD);
//    if (VM.VERIFY_ASSERTIONS) {
//      Address chunk = EmbeddedMetaData.getMetaDataBase(addr);
//      VM.assertions._assert(markWord.GE(chunk));
//      VM.assertions._assert(markWord.LT(chunk.plus(Region.BYTES_IN_MARKTABLE)));
//    }
//    Word mask = Word.one().lsh(bitIndex & (BITS_IN_WORD - 1));
//    Word oldValue, newValue;
//    do {
//      oldValue = markWord.prepareWord();
//      if (!oldValue.and(mask).isZero()) return false;
//      newValue = oldValue.or(mask);
//    } while (!markWord.attempt(oldValue, newValue));
//    return true;
//  }
//
  @Inline
  public static boolean isMarkedPrev(ObjectReference object) {
    Address table = getPrevTable(Region.of(object));
    return liveBitSet(VM.objectModel.refToAddress(object), table);
  }
  @Inline
  public static boolean isMarkedNext(ObjectReference object) {
    Address table = getNextTable(Region.of(object));
    return liveBitSet(VM.objectModel.refToAddress(object), table);
  }

  @Inline
  public static boolean testAndMarkPrev(ObjectReference object) {
    Address table = getPrevTable(Region.of(object));
    boolean x = setLiveBit(VM.objectModel.refToAddress(object), table, true);
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(isMarkedPrev(object));
    return x;
  }

  @Inline
  public static boolean testAndMarkNext(ObjectReference object) {
    Address table = getNextTable(Region.of(object));
    boolean x = setLiveBit(VM.objectModel.refToAddress(object), table, true);
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(isMarkedNext(object));
    return x;
  }

  @Inline
  private static boolean setLiveBit(Address address, Address table, boolean atomic) {
    Word oldValue;
    Address liveWord = getLiveWordAddress(address, table);
    Word mask = getMask(address);
    if (atomic) {
      do {
        oldValue = liveWord.prepareWord();
        if (oldValue.or(mask).EQ(oldValue)) return false;
      } while (!liveWord.attempt(oldValue, oldValue.or(mask)));
    } else {
      oldValue = liveWord.loadWord();
      liveWord.store(oldValue.or(mask));
    }

    return oldValue.and(mask).NE(mask);
  }
  @Inline
  private static boolean liveBitSet(Address address, Address table) {
    Address liveWord = getLiveWordAddress(address, table);
    Word mask = getMask(address);
    Word value = liveWord.loadWord();
    return value.and(mask).EQ(mask);
  }

  private static final Word WORD_SHIFT_MASK = Word.fromIntZeroExtend(BITS_IN_WORD).minus(Word.one());
  private static final int LOG_LIVE_COVERAGE = 2 + LOG_BITS_IN_BYTE;

  @Inline
  private static Word getMask(Address address) {
    int shift = address.toWord().rshl(LOG_BYTES_IN_WORD).and(WORD_SHIFT_MASK).toInt();
    return Word.one().lsh(shift);
  }
  @Inline
  private static Address getLiveWordAddress(Address address, Address base) {
//    Address chunk = EmbeddedMetaData.getMetaDataBase(address);
    Word diff = address.toWord().and(Region.REGION_MASK);
    Word bitIndex = diff.rshl(LOG_BYTES_IN_WORD);
    Word wordIndex = bitIndex.rshl(LOG_BITS_IN_WORD);
    Offset offset = wordIndex.lsh(LOG_BYTES_IN_WORD).toOffset();
    Address rtn = base.plus(offset);
//    Address rtn = base.plus(EmbeddedMetaData.getMetaDataOffset(address, LOG_LIVE_COVERAGE, LOG_BYTES_IN_WORD));
    if (VM.VERIFY_ASSERTIONS) {
      VM.assertions._assert(offset.toInt() < Region.MARK_BYTES_PER_REGION);
      VM.assertions._assert(rtn.GE(base));
      VM.assertions._assert(rtn.LT(base.plus(Region.MARK_BYTES_PER_REGION)));
    }
//    if (VM.VERIFY_ASSERTIONS) {
//      VM.assertions._assert(rtn.LT(base.plus(Region.BYTES_IN_MARKTABLE)));
//      Address region = Region.of(address);
//      int index = Region.indexOf(region) + 1;
//      Address start = chunk.plus(index * Region.MARK_BYTES_PER_REGION);
//      VM.assertions._assert(rtn.GE(start));
//      Address end = start.plus(Region.MARK_BYTES_PER_REGION);
//      VM.assertions._assert(rtn.LT(end));
//    }
    return rtn;
  }

  @Inline
  private static Address getPrevTable(Address region) {
    Address chunk = EmbeddedMetaData.getMetaDataBase(region);
    Address markTableBase;
    if (Region.getInt(region, Region.MD_ACTIVE_MARKTABLE) == 0) {
      markTableBase = chunk.plus(Region.MARKTABLE1_OFFSET);
    } else {
      markTableBase = chunk.plus(Region.MARKTABLE0_OFFSET);
    }
    int index = Region.indexOf2(region);
    Address start = markTableBase.plus(index * Region.MARK_BYTES_PER_REGION);
    return start;
  }


  @Inline
  private static Address getNextTable(Address region) {
    Address chunk = EmbeddedMetaData.getMetaDataBase(region);
    Address markTableBase;
    if (Region.getInt(region, Region.MD_ACTIVE_MARKTABLE) == 0) {
      markTableBase = chunk.plus(Region.MARKTABLE0_OFFSET);
    } else {
      markTableBase = chunk.plus(Region.MARKTABLE1_OFFSET);
    }
    int index = Region.indexOf2(region);
    Address start = markTableBase.plus(index * Region.MARK_BYTES_PER_REGION);
    return start;
  }

  @Inline
  public static void shiftTables(Address region) {
    int activeTable = Region.getInt(region, Region.MD_ACTIVE_MARKTABLE);
    Region.set(region, Region.MD_ACTIVE_MARKTABLE, 1 - activeTable);
    clearNextTable(region);
  }

  @Inline
  public static void clearNextTable(Address region) {
    Address table = getNextTable(region);
    VM.memory.zero(false, table, Extent.fromIntZeroExtend(Region.MARK_BYTES_PER_REGION));
  }

  @Inline
  public static void clearAllTables(Address region) {
    Address chunk = EmbeddedMetaData.getMetaDataBase(region);
    int index = Region.indexOf2(region);
    int offset = index * Region.MARK_BYTES_PER_REGION;
    {
      Address table = chunk.plus(Region.MARKTABLE0_OFFSET).plus(offset);
      VM.memory.zero(false, table, Extent.fromIntZeroExtend(Region.MARK_BYTES_PER_REGION));
    }
    {
      Address table = chunk.plus(Region.MARKTABLE1_OFFSET).plus(offset);
      VM.memory.zero(false, table, Extent.fromIntZeroExtend(Region.MARK_BYTES_PER_REGION));
    }
  }
}
