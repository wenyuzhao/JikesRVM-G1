package org.mmtk.plan.concurrent.pureg1;

import org.mmtk.policy.CardTable;
import org.mmtk.policy.MarkBlock;
import org.mmtk.policy.RemSet;
import org.mmtk.vm.VM;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.AddressArray;

public class PauseTimePredictor {
  public static float VFixed, U, S, C; // Milliseconds
  public static final int EXPECTED_PAUSE_TIME = 1000;


  public static float gcCost(int d, int rsSize, int liveBytes) {
    return VFixed + U * d + S * rsSize + C * liveBytes;
  }

  public static AddressArray predict(AddressArray cset) {
    // Calculate dirty cards
    int d = CardTable.dirtyCardSize();
    // Calculate rsSize & liveBytes
    int rsSize = 0, liveBytes = 0, cursor = 0;
    for (int i = 0; i < cset.length(); i++) {
      Address block = cset.get(i);
      if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!block.isZero());
      int newLiveBytes = liveBytes + MarkBlock.usedSize(block);
      int newRSSize = rsSize + MarkBlock.remSetSize(block);
      if (gcCost(d, newRSSize, newLiveBytes) > EXPECTED_PAUSE_TIME) {
        break;
      } else {
        cursor += 1;
        rsSize = newRSSize;
        liveBytes = newLiveBytes;
      }
    }
    // Remove blocks with index > cursor
    for (int i = cursor + 1; i < cset.length(); i++) {
      cset.set(i, Address.zero());
    }
    return cset;
  }
}
