package org.mmtk.plan.concurrent.g1;

import org.mmtk.policy.CardTable;
import org.mmtk.policy.Region;
import org.mmtk.policy.RegionSpace;
import org.mmtk.policy.RemSet;
import org.mmtk.utility.Log;
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

  @Inline public static void nurseryGCStart() {
    startTime = VM.statistics.nanoTime();
    dirtyCards = CardTable.dirtyCardSize();
  }

  static float totalNurseryCount;
  static float totalNurseryCardScanningTime;
  static float totalNurseryEvacuationTime;
  static float totalNurseryRegions;

  @Inline public static void nurseryGCEnd() {
    float totalTime = (float) (VM.statistics.nanoTime() - startTime);
    float cardScanningTime = U * dirtyCards / G1.parallelWorkers.activeWorkerCount();
    totalNurseryCount += 1;
    totalNurseryCardScanningTime += cardScanningTime;
    totalNurseryEvacuationTime += totalTime - cardScanningTime;
    totalNurseryRegions += G1.relocationSet.length();

    float averageCardScanningTime = totalNurseryCardScanningTime / totalNurseryCount;
    float evacuationTimePerRegion = totalNurseryEvacuationTime / totalNurseryRegions;
    int newEdenRegions = (int) ((EXPECTED_PAUSE_TIME() - averageCardScanningTime) / evacuationTimePerRegion);
    if (VM.VERIFY_ASSERTIONS) {
      Log.write("newEdenRegions0=");
      Log.writeln(newEdenRegions);
    }
    int maxEdenRegions = (int) ((Options.g1MaxNewSizePercent.getValue() / 100f) * global().TOTAL_LOGICAL_REGIONS);
    if (newEdenRegions > maxEdenRegions) newEdenRegions = maxEdenRegions;
    if (newEdenRegions < 1) newEdenRegions = 1;
    global().newSizeRatio = ((float) newEdenRegions) / ((float) global().TOTAL_LOGICAL_REGIONS);
    if (VM.VERIFY_ASSERTIONS) {
      Log.write("averageCardScanningTime=");
      Log.writeln(averageCardScanningTime);
      Log.write("averageCardScanningTime=");
      Log.writeln(averageCardScanningTime);
      Log.write("evacuationTimePerRegion=");
      Log.writeln(evacuationTimePerRegion);
      Log.write("newEdenRegions=");
      Log.writeln(newEdenRegions);
      Log.write("NewSizeRatio=");
      Log.writeln(global().newSizeRatio);
    }
  }


  public static AddressArray predict(AddressArray cset) {
    int rsSize = 0, liveBytes = 0;
    int expectedPauseTime = EXPECTED_PAUSE_TIME();

    int cursor = 0;
    for (int i = 0; i < cset.length(); i++) {
      Address block = cset.get(i);
      if (block.isZero()) continue;
      int newLiveBytes = liveBytes + Region.usedSize(block);
      int newRSSize = rsSize + Region.metaDataOf(block, Region.METADATA_REMSET_SIZE_OFFSET).loadInt();
      if (/*!isNurseryRegion &&*/ gcCost(dirtyCards, newRSSize, newLiveBytes) > expectedPauseTime) {
//         cset.set(i, Address.zero());
        break;
      } else {
        cursor++;
        rsSize = newRSSize;
        liveBytes = newLiveBytes;
      }
    }

    for (int i = cursor; i < cset.length(); i++) {
      cset.set(i, Address.zero());
    }

    totalRSSize = rsSize;
    totalLiveBytes = liveBytes;

    return cset;
  }

  @Inline
  private static G1 global() {
    return (G1) VM.activePlan.global();
  }
}
