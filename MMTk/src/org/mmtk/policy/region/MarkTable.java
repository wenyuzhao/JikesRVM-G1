package org.mmtk.policy.region;

import org.mmtk.utility.alloc.EmbeddedMetaData;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.ObjectReference;
import org.vmmagic.unboxed.Word;

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
  public static boolean isMarked(ObjectReference object) {
    return liveBitSet(VM.objectModel.refToAddress(object));
  }
  @Inline
  public static boolean testAndMark(ObjectReference object) {
    return setLiveBit(VM.objectModel.refToAddress(object), true);
  }
  @Inline
  public static boolean writeMarkState(ObjectReference object) {
    return setLiveBit(VM.objectModel.refToAddress(object), false);
  }
  @Inline
  private static boolean setLiveBit(Address address, boolean atomic) {
    Word oldValue;
    Address liveWord = getLiveWordAddress(address);
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
  protected static boolean liveBitSet(Address address) {
    Address liveWord = getLiveWordAddress(address);
    Word mask = getMask(address);
    Word value = liveWord.loadWord();
    return value.and(mask).EQ(mask);
  }

  private static final Word WORD_SHIFT_MASK = Word.fromIntZeroExtend(BITS_IN_WORD).minus(Word.one());
  private static final int LOG_LIVE_COVERAGE = 2 + LOG_BITS_IN_BYTE;

  @Inline
  private static Word getMask(Address address) {
    int shift = address.toWord().rshl(2).and(WORD_SHIFT_MASK).toInt();
    return Word.one().lsh(shift);
  }
  @Inline
  private static Address getLiveWordAddress(Address address) {
    Address chunk = EmbeddedMetaData.getMetaDataBase(address);
    Address rtn = chunk.plus(EmbeddedMetaData.getMetaDataOffset(address, LOG_LIVE_COVERAGE, LOG_BYTES_IN_WORD));
    if (VM.VERIFY_ASSERTIONS) {
      VM.assertions._assert(rtn.LT(chunk.plus(Region.BYTES_IN_MARKTABLE)));
      Address region = Region.of(address);
      int index = Region.indexOf(region) + 1;
      Address start = chunk.plus(index * Region.MARK_BYTES_PER_REGION);
      VM.assertions._assert(rtn.GE(start));
      Address end = start.plus(Region.MARK_BYTES_PER_REGION);
      VM.assertions._assert(rtn.LT(end));
    }
    return rtn;
  }

}
