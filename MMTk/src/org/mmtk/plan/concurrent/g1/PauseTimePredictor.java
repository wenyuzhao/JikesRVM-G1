package org.mmtk.plan.concurrent.g1;

import org.mmtk.policy.CardTable;
import org.mmtk.policy.Region;
import org.mmtk.policy.RegionSpace;
import org.mmtk.policy.RemSet;
import org.mmtk.utility.options.MaxGCPauseMillis;
import org.mmtk.utility.options.Options;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.AddressArray;
import org.vmmagic.unboxed.ObjectReference;
import org.vmmagic.unboxed.Offset;

@Uninterruptible
public class PauseTimePredictor {
  public static float VFixed = 100000000f, U = 24536f, S = 729f, C = 42f; // Nanoseconds
  @Inline
  private static final int EXPECTED_PAUSE_TIME() {
    return Options.maxGCPauseMillis.getValue() * 1000000; // Nanoseconds
  }

  static {
    Options.maxGCPauseMillis = new MaxGCPauseMillis();
  }

  public static float gcCost(int d, int rsSize, int liveBytes) {
//    float VFixed = liveBytes * a + b;
    return VFixed + (U * d) / (G1.parallelWorkers.activeWorkerCount()) + S * rsSize + C * liveBytes;
  }

  /** Runtime information of current pause */
  private static long startTime = 0;
  private static int dirtyCards = 0, totalRSSize = 0, totalLiveBytes = 0;
  private static long[] UData = new long[] { 0, 0 };
  private static long[] SData = new long[] { 0, 0 };
  private static long[] CData = new long[] { 0, 0 };

  @Inline public static void updateRefinementCardScanningTime(long ns) {
    long oldValue, newValue;
    do {
      oldValue = UData[0];
      newValue = oldValue + ns;
    } while (!VM.memory.attemptLong(UData, Offset.fromIntZeroExtend(0), oldValue, newValue));
    do {
      oldValue = UData[1];
      newValue = oldValue + 1;
    } while (!VM.memory.attemptLong(UData, Offset.fromIntZeroExtend(8), oldValue, newValue));
  }
  @Inline public static void updateRemSetCardScanningTime(long ns) {
    long oldValue, newValue;
    do {
      oldValue = SData[0];
      newValue = oldValue + ns;
    } while (!VM.memory.attemptLong(SData, Offset.fromIntZeroExtend(0), oldValue, newValue));
    do {
      oldValue = SData[1];
      newValue = oldValue + 1;
    } while (!VM.memory.attemptLong(SData, Offset.fromIntZeroExtend(8), oldValue, newValue));
  }
  @Inline public static void updateObjectEvacuationTime(ObjectReference ref, long ns) {
    long oldValue, newValue;
    do {
      oldValue = CData[0];
      newValue = oldValue + ns;
    } while (!VM.memory.attemptLong(CData, Offset.fromIntZeroExtend(0), oldValue, newValue));
    do {
      oldValue = CData[1];
      newValue = oldValue + VM.objectModel.getSizeWhenCopied(ref);
    } while (!VM.memory.attemptLong(CData, Offset.fromIntZeroExtend(8), oldValue, newValue));
  }

  public static final RegionSpace.EvacuationTimer evacuationTimer = new RegionSpace.EvacuationTimer() {
    @Override
    @Inline
    @Uninterruptible
    public void updateObjectEvacuationTime(ObjectReference ref, long ns) {
      PauseTimePredictor.updateObjectEvacuationTime(ref, ns);
    }
  };

  public static final RemSet.RemSetCardScanningTimer remSetCardScanningTimer = new RemSet.RemSetCardScanningTimer() {
    @Override
    @Inline
    @Uninterruptible
    public void updateRemSetCardScanningTime(long ns) {
      PauseTimePredictor.updateRemSetCardScanningTime(ns);
    }
  };

//  private static int[] UData = new int[] { 0, 0 };
//  private static int[] SData = new int[] { 0, 0 };
//  private static int[] CData = new int[] { 0, 0 };
//
//  @Inline public static void updateRefinementCardScanningTime(long ns) {
//    Address totalMS = ObjectReference.fromObject(UData).toAddress();
//    Address count = totalMS.plus(Constants.BYTES_IN_LONG);
//    int oldValue, newValue;
//    do {
//      oldValue = totalMS.prepareInt();
//      newValue = oldValue + (int) ns;
//    } while (!totalMS.attempt(oldValue, newValue));
//    do {
//      oldValue = count.prepareInt();
//      newValue = oldValue + 1;
//    } while (!count.attempt(oldValue, newValue));
//
//  }
//  @Inline public static void updateRemSetCardScanningTime(long ns) {
//    Address totalMS = ObjectReference.fromObject(SData).toAddress();
//    Address count = totalMS.plus(Constants.BYTES_IN_INT);
//    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(((int) ns) >= 0);
//    int oldValue, newValue;
//    do {
//      oldValue = totalMS.prepareInt();
//      newValue = oldValue + (int) ns;
//    } while (!totalMS.attempt(oldValue, newValue));
//    do {
//      oldValue = count.prepareInt();
//      newValue = oldValue + 1;
//    } while (!count.attempt(oldValue, newValue));
//  }
//  @Inline public static void updateObjectEvacuationTime(ObjectReference ref, long ns) {
//    Address totalMS = ObjectReference.fromObject(CData).toAddress();
//    Address totalBytes = totalMS.plus(Constants.BYTES_IN_INT);
//    int oldValue, newValue;
//    do {
//      oldValue = totalMS.prepareInt();
//      newValue = oldValue + (int) ns;
//    } while (!totalMS.attempt(oldValue, newValue));
//    do {
//      oldValue = totalBytes.prepareInt();
//      newValue = oldValue + VM.objectModel.getSizeWhenCopied(ref);
//    } while (!totalBytes.attempt(oldValue, newValue));
//  }


  @Inline public static void stopTheWorldStart() {
    startTime = VM.statistics.nanoTime();
    dirtyCards = CardTable.dirtyCardSize();
  }

  /** Update prediction parameters */
  static float a = 0, b = 0;
  @Inline public static void stopTheWorldEnd() {
    float totalTime = (float) (VM.statistics.nanoTime() - startTime);
    U = UData[1] == 0 ? U : (UData[0] / UData[1]);
    S = SData[1] == 0 ? S : (SData[0] / SData[1]);
    C = CData[1] == 0 ? C : (CData[0] / CData[1]);
    float newFixedTime = totalTime - (U * dirtyCards) / (G1.parallelWorkers.activeWorkerCount()) - S * totalRSSize - C * totalLiveBytes;
//    a = (float) fixedTime / totalLiveBytes;
    VFixed = (VFixed * 0.2f + newFixedTime * 0.8f);
    if (VFixed < 0) VFixed = 0;
//    if (VM.VERIFY_ASSERTIONS) {
//      Log.write("[GC pause time: ");
//      Log.write(totalTime);
//      Log.writeln(" ms]");
//    }
  }
  static boolean firstGC = true;
  public static AddressArray predict(AddressArray cset, short gcKind) {
    // Calculate dirty cards
    //int d = CardTable.dirtyCardSize();
    // Calculate rsSize & liveBytes


//    if (VM.VERIFY_ASSERTIONS) {
//      Log.write("U ");
//      Log.writeln(U);
//      Log.write("S ");
//      Log.writeln(S);
//      Log.write("C ");
//      Log.writeln(C);
//      Log.write("VFixed ");
//      Log.writeln(VFixed);
//      Log.write("dirtyCards ");
//      Log.writeln(dirtyCards);
//      Log.flush();
//    }
    int rsSize = 0, liveBytes = 0;
    int expectedPauseTime = EXPECTED_PAUSE_TIME();

    if (gcKind == G1.YOUNG_GC) {
      // cset only contains young regions
      // choose young regions
      int cursor = 0;
      for (int i = 0; i < cset.length(); i++) {
        Address block = cset.get(i);
        int newLiveBytes = liveBytes + Region.usedSize(block);
        int newRSSize = rsSize + Region.metaDataOf(block, Region.METADATA_REMSET_SIZE_OFFSET).loadInt();

//        if (VM.VERIFY_ASSERTIONS) {
//          Log.write("Block (", i);
//          Log.write("/", cset.length());
//          Log.write(") ", block);
//          Log.write(": ", Region.usedSize(block));
//          Log.write("/", Region.BYTES_IN_BLOCK);
//          Log.write(" rsSize ", Region.metaDataOf(block, Region.METADATA_REMSET_SIZE_OFFSET).loadInt());
//          Log.write(" liveBytes ", Region.metaDataOf(block, Region.METADATA_ALIVE_SIZE_OFFSET).loadInt());
//          Log.writeln(" relocate");
//        }
        if (gcCost(dirtyCards, newRSSize, newLiveBytes) > expectedPauseTime) {
          break;
        } else {
          cursor += 1;
          rsSize = newRSSize;
          liveBytes = newLiveBytes;
        }
      }
//      if (firstGC) {
//        if (cursor >= PureG1.parallelWorkers.activeWorkerCount()) {
//          cursor = PureG1.parallelWorkers.activeWorkerCount();
//        }
//        firstGC = false;
//      }

//      if (VM.VERIFY_ASSERTIONS) {
//        Log.write("Expected pause time: ");
//        Log.writeln(gcCost(dirtyCards, rsSize, liveBytes));
//      }

      for (int i = cursor; i < cset.length(); i++) {
        cset.set(i, Address.zero());
      }
    } else {

      int cursor = 0;
      for (int i = 0; i < cset.length(); i++) {
        Address block = cset.get(i);
        if (block.isZero()) continue;
        int newLiveBytes = liveBytes + Region.usedSize(block);
        int newRSSize = rsSize + Region.metaDataOf(block, Region.METADATA_REMSET_SIZE_OFFSET).loadInt();

//        if (VM.VERIFY_ASSERTIONS) {
//          Log.write("Block (", i);
//          Log.write("/", cset.length());
//          Log.write(") ", block);
//          Log.write(": ", Region.usedSize(block));
//          Log.write("/", Region.BYTES_IN_BLOCK);
//          Log.write(" rsSize ", Region.metaDataOf(block, Region.METADATA_REMSET_SIZE_OFFSET).loadInt());
//          Log.write(" liveBytes ", Region.metaDataOf(block, Region.METADATA_ALIVE_SIZE_OFFSET).loadInt());
//          Log.writeln(" relocate");
//        }
//        boolean isNurseryRegion = Region.metaDataOf(block, Region.METADATA_GENERATION_OFFSET).loadInt() == 0;
        if (/*!isNurseryRegion &&*/ gcCost(dirtyCards, newRSSize, newLiveBytes) > expectedPauseTime) {
//          cset.set(i, Address.zero());
          break;
        } else {
          cursor++;
          rsSize = newRSSize;
          liveBytes = newLiveBytes;
        }
      }

//      if (VM.VERIFY_ASSERTIONS) {
//        Log.write("Expected pause time: ");
//        Log.writeln(gcCost(dirtyCards, rsSize, liveBytes));
//        Log.write("VFixed ");
//        Log.writeln(VFixed);
//        Log.write("U ");
//        Log.writeln(U);
//        Log.write("S ");
//        Log.writeln(S);
//        Log.write("C ");
//        Log.writeln(C);
//        Log.write("dirtyCards ");
//        Log.writeln(dirtyCards);
//        Log.write("rsSize ");
//        Log.writeln(rsSize);
//        Log.write("liveBytes ");
//        Log.writeln(liveBytes);
//      }

      for (int i = cursor; i < cset.length(); i++) {
        cset.set(i, Address.zero());
      }
//        int nonNullCursor = 0;
//        for (int j = 0; j < cset.length(); j++) {
//          Address a = cset.get(j);
//          if (!a.isZero()) {
//            cset.set(nonNullCursor++, a);
//          }
//        }
//        for (int j = nonNullCursor; j < cset.length(); j++) {
//          cset.set(nonNullCursor++, Address.zero());
//        }
//      }

    }

    totalRSSize = rsSize;
    totalLiveBytes = liveBytes;

    return cset;
    /*int rsSize = 0, liveBytes = 0, cursor = 0;
    for (int i = 0; i < cset.length(); i++) {
      Address block = cset.get(i);
      //if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!block.isZero());
      int newLiveBytes = liveBytes + Region.usedSize(block);
      int newRSSize = rsSize + Region.metaDataOf(block, Region.METADATA_REMSET_SIZE_OFFSET).loadInt();

      if (VM.VERIFY_ASSERTIONS) {
        Log.write("Block (", i);
        Log.write("/", cset.length());
        Log.write(") ", block);
        Log.write(": ", Region.usedSize(block));
        Log.write("/", Region.BYTES_IN_BLOCK);
        Log.write(" rsSize ", Region.metaDataOf(block, Region.METADATA_REMSET_SIZE_OFFSET).loadInt());
        Log.write(" liveBytes ", Region.metaDataOf(block, Region.METADATA_ALIVE_SIZE_OFFSET).loadInt());
        Log.writeln(" relocate");
      }
      boolean isNurseryRegion = Region.metaDataOf(block, Region.METADATA_GENERATION_OFFSET).loadInt() == 0;
      if (!isNurseryRegion && gcCost(dirtyCards, newRSSize, newLiveBytes) > EXPECTED_PAUSE_TIME) {
        break;
      } else {
        cursor += 1;
        rsSize = newRSSize;
        liveBytes = newLiveBytes;
      }
    }

    Log.writeln("Cursor ", cursor);
    // Remove blocks with index > cursor
    for (int i = cursor + 1; i < cset.length(); i++) {
      cset.set(i, Address.zero());
    }
    //
    totalRSSize = rsSize;
    totalLiveBytes = liveBytes;
    return cset;
    */
  }
}
