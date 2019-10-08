package org.mmtk.plan.g1;

import org.mmtk.policy.region.CardTable;
import org.mmtk.policy.region.Region;
import org.mmtk.policy.region.Stat;
import org.mmtk.utility.Log;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;

/**
 * V(cs) = V_{fixed} + U*d + \sum_{r \in cs}{( S*rsSize(r) + C*liveBytes(r) )}
 *
 * - V(cs)        is the cost of collecting collection set cs;
 * - V_{fixed}    represents fixed costs, common to all pauses;
 * - U            is the average cost of scanning a card
 * - d            is the number of dirty cards that must be scanned to bring remembered sets up-to-date;
 * - S            is the of scanning a card from a remembered set for pointers into the collection set
 * - rsSize(r)    is the number of card entries in r’s remembered set;
 * - C            is the cost per byte of evacuating (and scanning) a live object,
 * - liveBytes(r) is an estimate of the number of live bytes in region r.
 */
@Uninterruptible
public class PauseTimePredictor {
  static final long PAUSE_TIME_GOAL = VM.statistics.millisToNanos(50); // ms

  // Parameters
  long V_fixed;
  double U, S, C;
  public final Stat stat = new Stat();

  // Per-GC data
  double V_cs;

  public void prepare() {
    stat.pauseStart(CardTable.numDirtyCards());
    Log.writeln("Dirty cards ", CardTable.numDirtyCards());
    Log.write("U = "); Log.writeln(U);
    Log.writeln("Ud = ", (long) (U * CardTable.numDirtyCards()));
    Log.write("V_cs = "); Log.writeln(V_cs);
    V_cs = V_fixed + (long) (U * CardTable.numDirtyCards());
  }

  /// Return true if within pause time goal
  @Inline
  public boolean predict(Address region, boolean alwaysIncludeInCSet) {
    double delta = S * Region.getInt(region, Region.MD_REMSET_SIZE) + C * Region.liveBytes(region);
    Log.write("Region ", region);
    Log.write(" oldvcs ");
    Log.write(V_cs);
    Log.write(" delta ");
    Log.writeln(delta);
    if (alwaysIncludeInCSet) {
      V_cs += delta;
      return true;
    } else {
      if (V_cs + delta > PAUSE_TIME_GOAL) return false;
      V_cs += delta;
      return true;
    }
  }

  // Update parameters
  public void release() {
    stat.pauseEnd();
    V_fixed = (V_fixed + stat.V_fixed) / 2;
    U = mix(U, stat.U);
    S = mix(S, stat.S);
    C = mix(C, stat.C);
  }

  @Inline
  private static double mix(double a, double b) {
    return (a + b) / 2;
  }
}
