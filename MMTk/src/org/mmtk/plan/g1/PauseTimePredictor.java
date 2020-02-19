package org.mmtk.plan.g1;

import org.mmtk.policy.region.CardTable;
import org.mmtk.policy.region.Region;
import org.mmtk.policy.region.RemSet;
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
  static final long PAUSE_TIME_GOAL = VM.statistics.millisToNanos(15); // ms

  // Parameters
  float nurseryRatio = G1.constraints().g1FixedNurseryRatio();
  long V_fixed;
  double U, S, C;
  double nurserySurvivorRate;
  double nurseryRemsetCards, nurseryDirtyCards;
  public final Stat stat = new Stat();

  // Per-GC data
  double V_cs;

  public void prepare() {
    if (!G1.ENABLE_PAUSE_TIME_PREDICTOR) return;
    stat.pauseStart(CardTable.numDirtyCards());
    V_cs = V_fixed + (long) (U * CardTable.numDirtyCards());
  }

  /// Return true if within pause time goal
  @Inline
  public boolean predict(Address region, boolean alwaysIncludeInCSet) {
    if (!G1.ENABLE_PAUSE_TIME_PREDICTOR) return true;
    int cards = RemSet.calculateRememberedCards(region);
    double delta = S * cards + C * Region.liveBytes(region);
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
  public void release(boolean nursery) {
    if (!G1.ENABLE_PAUSE_TIME_PREDICTOR) return;
    stat.pauseEnd();
    if (nursery) {
      V_fixed = mix(V_fixed, stat.V_fixed);
      U = mix(U, stat.U);
      S = mix(S, stat.S);
      C = mix(C, stat.C);

      double survivorRate = ((double) stat.nurserySurvivedBytes.get()) / ((double) (G1.regionSpace.maxRegions() * nurseryRatio * Region.BYTES_IN_REGION));
      nurserySurvivorRate = mix(nurserySurvivorRate, survivorRate);

      double copyTimePerRegion = Region.BYTES_IN_REGION * survivorRate * stat.C;
      nurseryRemsetCards = (nurseryRemsetCards + (double) stat.totalRemSetCards.get()) / 2;
      nurseryDirtyCards = (nurseryDirtyCards + (double) stat.totalRefineCards) / 2;
      double fixedTime = V_fixed;
      fixedTime += nurseryDirtyCards * U + nurseryRemsetCards * C;
      double nurserySizeAwareTime = ((double) PAUSE_TIME_GOAL) - fixedTime;
      double newNurseryRatio = nurserySizeAwareTime / copyTimePerRegion;
      nurseryRatio = (nurseryRatio + (float) newNurseryRatio) / 2;
//      if (nurserySizeAwareTime < 0) return;
//      long budget = PAUSE_TIME_GOAL - stat.V_fixed - stat.totalRefineTime;
//      if (budget < 0) budget = 0;
//      nurseryRatio = (float) (nurseryRatio * (((double) budget) / ((double) nurserySizeAwareTime)));
      if (nurseryRatio < 0.05f) nurseryRatio = 0.05f;
      if (nurseryRatio > 0.40f) nurseryRatio = 0.40f;
    } else {
      V_fixed = mix(V_fixed, stat.V_fixed);
      U = mix(U, stat.U);
      S = mix(S, stat.S);
      C = mix(C, stat.C);
    }
  }

  @Inline
  private static long mix(long a, long b) {
    if (a == 0) return b;
    return (a + b) / 2;
  }

  @Inline
  private static double mix(double a, double b) {
    if (a == 0) return b;
    return (a + b) / 2;
  }
}
