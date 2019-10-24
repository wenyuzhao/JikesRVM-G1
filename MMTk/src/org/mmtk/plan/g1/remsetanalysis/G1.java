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
package org.mmtk.plan.g1.remsetanalysis;

import org.mmtk.plan.ComplexPhase;
import org.mmtk.plan.Phase;
import org.mmtk.policy.region.Region;
import org.mmtk.policy.region.RemSet;
import org.mmtk.utility.Constants;
import org.mmtk.utility.Log;
import org.mmtk.utility.statistics.DoubleCounter;
import org.mmtk.utility.statistics.DoubleEventCounter;
import org.mmtk.utility.statistics.EventCounter;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;

@Uninterruptible
public class G1 extends org.mmtk.plan.g1.G1 {
  static final DoubleCounter remsetAccumulatedFootprint;
  static final DoubleCounter remsetAccumulatedUtilization;

  static {
    remsetAccumulatedFootprint = new DoubleCounter("remset.accumulated.footprint", true, true, true);
    remsetAccumulatedUtilization = new DoubleCounter("remset.accumulated.utilization", true, true, true);
  }


  private void statRemSet() {
    int remsetUsedBytes = 0, remsetRecordedCards = 0, remsetRegionSpaceUsedBytes = 0;
    for (Address region = regionSpace.firstRegion(); !region.isZero(); region = Region.getNext(region)) {
      remsetUsedBytes += RemSet.committedBytes(region);
      remsetRecordedCards += RemSet.rememberedCards(region);
      remsetRegionSpaceUsedBytes += Region.BYTES_IN_REGION;
    }

    double footprint = ((double) remsetUsedBytes) / ((double) remsetRegionSpaceUsedBytes);
    double utilization = ((double) remsetRecordedCards) / ((double) (remsetUsedBytes * Constants.BITS_IN_BYTE));

    remsetAccumulatedFootprint.inc(footprint * 100);
    remsetAccumulatedUtilization.inc(utilization * 100);
  }

  @Override
  public void collectionPhase(short phaseId) {
    if (phaseId == STAT_REMSET) {
      statRemSet();
      return;
    }
    super.collectionPhase(phaseId);
  }

  @Override
  public void processOptions() {
    super.processOptions();
    {
      int oldClosure = Phase.schedulePlaceholder(STAT_REMSET);
      int newClosure = Phase.scheduleGlobal(STAT_REMSET);
      ComplexPhase cp = (ComplexPhase) Phase.getPhase(mixedCollection);
      cp.replacePhase(oldClosure, newClosure);
    }
    {
      int oldClosure = Phase.schedulePlaceholder(STAT_REMSET);
      int newClosure = Phase.scheduleGlobal(STAT_REMSET);
      ComplexPhase cp = (ComplexPhase) Phase.getPhase(nurseryCollection);
      cp.replacePhase(oldClosure, newClosure);
    }
  }
}
