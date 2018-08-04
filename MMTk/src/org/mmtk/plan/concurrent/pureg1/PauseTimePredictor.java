package org.mmtk.plan.concurrent.pureg1;

import org.mmtk.policy.CardTable;
import org.mmtk.policy.Region;
import org.mmtk.utility.Constants;
import org.mmtk.utility.Log;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.AddressArray;
import org.vmmagic.unboxed.ObjectReference;

@Uninterruptible
public class PauseTimePredictor {
  public static float VFixed, U, S, C; // Milliseconds
  public static final int EXPECTED_PAUSE_TIME = 400;

  public static float gcCost(int d, int rsSize, int liveBytes) {
    float VFixed = liveBytes * a + b;
    return VFixed + U * d + S * rsSize + C * liveBytes;
  }

  /** Runtime information of current pause */
  private static long startTime = 0;
  private static int dirtyCards = 0, totalRSSize = 0, totalLiveBytes = 0;
  private static int[] UData = new int[2];
  private static int[] SData = new int[2];
  private static int[] CData = new int[2];

  @Inline public static void updateRefinementCardScanningTime(long ns) {
    Address totalMS = ObjectReference.fromObject(UData).toAddress();
    Address count = totalMS.plus(Constants.BYTES_IN_INT);
    int oldValue, newValue;
    do {
      oldValue = totalMS.prepareInt();
      newValue = oldValue + (int) ns;
    } while (!totalMS.attempt(oldValue, newValue));
    do {
      oldValue = count.prepareInt();
      newValue = oldValue + 1;
    } while (!count.attempt(oldValue, newValue));

  }
  @Inline public static void updateRemSetCardScanningTime(long ns) {
    Address totalMS = ObjectReference.fromObject(SData).toAddress();
    Address count = totalMS.plus(Constants.BYTES_IN_INT);
    int oldValue, newValue;
    do {
      oldValue = totalMS.prepareInt();
      newValue = oldValue + (int) ns;
    } while (!totalMS.attempt(oldValue, newValue));
    do {
      oldValue = count.prepareInt();
      newValue = oldValue + 1;
    } while (!count.attempt(oldValue, newValue));
  }
  @Inline public static void updateObjectEvacuationTime(ObjectReference ref, long ns) {
    //if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!ref.isNull());
    Address totalMS = ObjectReference.fromObject(CData).toAddress();
    Address totalBytes = totalMS.plus(Constants.BYTES_IN_INT);
    int oldValue, newValue;
    do {
      oldValue = totalMS.prepareInt();
      newValue = oldValue + (int) ns;
    } while (!totalMS.attempt(oldValue, newValue));
    do {
      oldValue = totalBytes.prepareInt();
      newValue = oldValue + VM.objectModel.getSizeWhenCopied(ref);
    } while (!totalBytes.attempt(oldValue, newValue));
  }


  @Inline public static void stopTheWorldStart() {
    startTime = VM.statistics.nanoTime();
    // Calculate dirty cards
    dirtyCards = CardTable.dirtyCardSize();
//    dirtyCards = CardTable.dirtyCardSize();

    /*PauseTimePredictor.dirtyCards = dirtyCards;
    totalRSSize = 0;
    for (int i = 0; i < cset.length(); i++) {
      Address region = cset.get(i);
      if (region.isZero()) break;
      totalRSSize += Region.metaDataOf(region, Region.METADATA_REMSET_SIZE_OFFSET).loadInt();
    }
    totalLiveBytes = 0;
    for (int i = 0; i < cset.length(); i++) {
      Address region = cset.get(i);
      if (region.isZero()) break;
      totalLiveBytes += Region.metaDataOf(region, Region.METADATA_ALIVE_SIZE_OFFSET).loadInt();
    }*/
  }

  /** Update prediction parameters */
  static float a = 0, b = 0;
  @Inline public static void stopTheWorldEnd() {
    double totalTime = VM.statistics.nanosToMillis(VM.statistics.nanoTime() - startTime);
    U = (float) VM.statistics.nanosToMillis((long) (UData[0] / (float) UData[1]));
    S = (float) VM.statistics.nanosToMillis((long) (SData[0] / (float) SData[1]));
    C = (float) VM.statistics.nanosToMillis((long) (CData[0] / (float) CData[1]));
    double fixedTime = totalTime - (U * dirtyCards + S * totalRSSize + C * totalLiveBytes);
    a = (float) fixedTime / totalLiveBytes;
    //VFixed = (VFixed + ((float) totalTime - (U * dirtyCards + S * totalRSSize + C * totalLiveBytes))) / 2;
    if (VM.VERIFY_ASSERTIONS) {
      Log.write("[GC pause time: ");
      Log.write(totalTime);
      Log.writeln(" ms]");
    }
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

    if (gcKind == PureG1.YOUNG_GC) {
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
        if (gcCost(dirtyCards, newRSSize, newLiveBytes) > EXPECTED_PAUSE_TIME) {
          break;
        } else {
          cursor += 1;
          rsSize = newRSSize;
          liveBytes = newLiveBytes;
        }
      }
      if (firstGC) {
        if (cursor >= PureG1.parallelWorkers.activeWorkerCount()) {
          cursor = PureG1.parallelWorkers.activeWorkerCount();
        }
        firstGC = false;
      }
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
        boolean isNurseryRegion = Region.metaDataOf(block, Region.METADATA_GENERATION_OFFSET).loadInt() == 0;
        if (/*!isNurseryRegion &&*/ gcCost(dirtyCards, newRSSize, newLiveBytes) > EXPECTED_PAUSE_TIME) {
//          cset.set(i, Address.zero());
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
