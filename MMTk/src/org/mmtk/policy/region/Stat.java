package org.mmtk.policy.region;

import org.mmtk.utility.Atomic;
import org.mmtk.utility.Log;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Uninterruptible;

@Uninterruptible
public class Stat {
  // Data collected during a GC pause
  public long startTime, endTime, totalTime;
  public long totalRefineCards = 0;
  public long totalRefineTime = 0;
  public Atomic.Int totalRemSetCards = new Atomic.Int();
  public long totalRemSetTime = 0;
  public int totalCopyBytes = 0;
  public long totalCopyTime = 0;
  public Atomic.Long nurserySurvivedBytes = new Atomic.Long();

  // Derived data
  // Only valid after a stat round is finished (at the end of a pause)
  public long V_fixed;
  public double U, S, C;

  public void pauseStart(int dirtyCards) {
    startTime = VM.statistics.nanoTime();
    totalRefineCards = dirtyCards;
    totalRefineTime = 0;
    totalRemSetCards.set(0);
    totalRemSetTime = 0;
    totalCopyBytes = 0;
    totalCopyTime = 0;
    nurserySurvivedBytes.set(0);
  }

  public void pauseEnd() {
    endTime =  VM.statistics.nanoTime();
    totalTime = endTime - startTime;
    long ud = totalRefineTime;
    long vs = totalRemSetTime;
    long vc = totalCopyTime;
    // Update params
    V_fixed = totalTime - ud - vs - vc;
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(totalTime >= 0);
    U = totalRefineCards == 0 ? 0 : (((double) ud) / (double) totalRefineCards);
    S = totalRemSetCards.get() == 0 ? 0 : (((double) vs) / (double) totalRemSetCards.get());
    C = totalCopyBytes == 0 ? 0 : (((double) vc) / (double) totalCopyBytes);
  }
}
