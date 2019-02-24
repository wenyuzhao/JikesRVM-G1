/*
 *  This file is part of the Jikes RVM project (http://jikesrvm.org).
 *
 *  This file is licensed to You under the Eclipse Public License (EPL);
 *  You may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *      http://www.opensource.org/licenses/eclipse-1.0.php
 *
 *  See the COPYRIGHT.txt file distributed with this work for information
 *  regarding copyright ownership.
 */
package org.mmtk.utility;

import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.ObjectReference;
import org.vmmagic.unboxed.Offset;
import org.vmmagic.unboxed.Word;

/**
 * This class provides generic support for operations over the GC byte
 * within each object's header word. Specifically this class manages
 * global status bits which cut across policies (for example the logging bit).<p>
 *
 * The general pattern for use of the GC byte is that the high order bits
 * successively reserved for global use, as necessary.  Any GC policy may use
 * those bits that are not reserved for global use.
 */
@Uninterruptible
public class HeaderByte {
  private static final int TOTAL_BITS = 8;

  public static final boolean NEEDS_UNLOGGED_BIT = VM.activePlan.constraints().needsLogBitInHeader();
  private static final int UNLOGGED_BIT_NUMBER = TOTAL_BITS - (NEEDS_UNLOGGED_BIT ? 1 : 0);
  public static final byte UNLOGGED_BIT = (byte) (1 << UNLOGGED_BIT_NUMBER);
  public static final int USED_GLOBAL_BITS = TOTAL_BITS - UNLOGGED_BIT_NUMBER;

  private static final Offset GC_HEADER_OFFSET = VM.objectModel.GC_HEADER_OFFSET();

  @Inline
  private static Word prepareAvailableBits(ObjectReference ref) {
    return VM.objectModel.prepareAvailableBits(ref);
//    return ref.toAddress().prepareWord(GC_HEADER_OFFSET);
  }

  @Inline
  private static void writeAvailableBitsWord(ObjectReference ref, Word word) {
    VM.objectModel.writeAvailableBitsWord(ref, word);
//    ref.toAddress().store(word, GC_HEADER_OFFSET);
  }

  @Inline
  private static boolean attemptAvailableBits(ObjectReference ref, Word oldValue, Word newValue) {
    return VM.objectModel.attemptAvailableBits(ref, oldValue, newValue);
//    return ref.toAddress().attempt(oldValue, newValue, GC_HEADER_OFFSET);
  }

  @Inline
  public static void markAsUnlogged(ObjectReference ref) {
    Word oldValue = prepareAvailableBits(ref);
    Word newValue;
    if (UNLOGGED_BIT_IS_ZERO) {
      newValue = oldValue.and(MASK.not());
    } else {
      newValue = oldValue.or(MASK);
    }
    writeAvailableBitsWord(ref, newValue);
  }

  private static boolean UNLOGGED_BIT_IS_ZERO = false;
  private static final Word MASK = Word.one().lsh(UNLOGGED_BIT_NUMBER);

  @Inline
  public static void flip() {
    UNLOGGED_BIT_IS_ZERO = !UNLOGGED_BIT_IS_ZERO;
  }

  @Inline
  public static boolean attemptUnlog(ObjectReference ref) {
    Word oldValue, newValue;
    do {
      oldValue = prepareAvailableBits(ref);
      if (UNLOGGED_BIT_IS_ZERO) {
        if (oldValue.and(MASK).isZero()) return false; // already marked as 0
        newValue = oldValue.and(MASK.not());
      } else {
        if (!oldValue.and(MASK).isZero()) return false; // already marked as 1
        newValue = oldValue.or(MASK);
      }
    } while (!attemptAvailableBits(ref, oldValue, newValue));
    return true;
  }

  @Inline
  public static boolean attemptLog(ObjectReference ref) {
    Word oldValue, newValue;
    do {
      oldValue = prepareAvailableBits(ref);
      if (UNLOGGED_BIT_IS_ZERO) {
        if (!oldValue.and(MASK).isZero()) return false; // already marked as 1
        newValue = oldValue.or(MASK);
      } else {
        if (oldValue.and(MASK).isZero()) return false; // already marked as 0
        newValue = oldValue.and(MASK.not());
      }
    } while (!attemptAvailableBits(ref, oldValue, newValue));
    return true;
  }

  /**
   * Mark an object as logged.  Since duplicate logging does
   * not raise any correctness issues, we do <i>not</i> worry
   * about synchronization and allow threads to race to log the
   * object, potentially including it twice (unlike reference
   * counting where duplicates would lead to incorrect reference
   * counts).
   *
   * @param ref The object to be marked as logged
   */
  public static void markAsLogged(ObjectReference ref) {
    Word oldValue = prepareAvailableBits(ref);
    Word newValue;
    if (UNLOGGED_BIT_IS_ZERO) {
      newValue = Word.fromIntZeroExtend(oldValue.toInt() | MASK.toInt());
    } else {
      newValue = Word.fromIntZeroExtend(oldValue.toInt() & ~MASK.toInt());
    }
    writeAvailableBitsWord(ref, newValue);
//    if ((oldValue.toInt() & ~0xff) != (newValue.toInt() & ~0xff)) {
//      Log.writeln("Old word ", oldValue);
//      Log.writeln("New word ", oldValue);
//      VM.assertions.fail("ERROR");
//    }
  }


  public static Word markWordAsLogged(Word oldValue) {
    if (UNLOGGED_BIT_IS_ZERO) {
      return oldValue.or(MASK);
    } else {
      return oldValue.and(MASK.not());
    }
  }

  public static byte markByteAsLogged(byte oldValue) {
    if (UNLOGGED_BIT_IS_ZERO) {
      return (byte) (oldValue | UNLOGGED_BIT);
    } else {
      return (byte) (oldValue & ~UNLOGGED_BIT);
    }
  }

  /**
   * Return {@code true} if the specified object needs to be logged.
   *
   * @param object The object in question
   * @return {@code true} if the object in question needs to be logged (remembered).
   */
  public static boolean isUnlogged(ObjectReference object) {
    Word oldValue = prepareAvailableBits(object);
    if (UNLOGGED_BIT_IS_ZERO) {
      if (oldValue.and(MASK).isZero()) return true; // already marked as 0
    } else {
      if (!oldValue.and(MASK).isZero()) return true; // already marked as 1
    }
    return false;
  }
}
