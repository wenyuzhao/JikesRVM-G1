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
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Interruptible;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;

@Uninterruptible
public class G1 extends org.mmtk.plan.g1.G1 {
  protected static final short statRemSet = Phase.createComplex("stat-remset", null,
      Phase.scheduleGlobal   (REFINE_CARDS),
      Phase.scheduleMutator  (REFINE_CARDS),
      Phase.scheduleCollector(REFINE_CARDS),
      Phase.scheduleGlobal   (STAT_REMSET)
  );

  static final DoubleCounter remsetFootprintOverMaxHeapSize;
  static final DoubleCounter remsetFootprintOverUsedHeapSize;
  static final DoubleCounter remsetUtilization;

  static {
    remsetFootprintOverMaxHeapSize = new DoubleCounter("remset.footprint.overMaxHeapSize", true, true, true);
    remsetFootprintOverUsedHeapSize = new DoubleCounter("remset.footprint.overUsedHeapSize", true, true, true);
    remsetUtilization = new DoubleCounter("remset.utilization", true, true, true);
  }


  private void statRemSet() {
    int committedBytes = 0, recordedCards = 0;
    for (Address region = regionSpace.firstRegion(); !region.isZero(); region = Region.getNext(region)) {
      committedBytes += RemSet.committedBytes(region);
      recordedCards += RemSet.rememberedCards(region);
    }
    int maxHeapSize = this.getTotalPages() * Constants.BYTES_IN_PAGE;
    int usedHeapSize = this.getPagesUsed() * Constants.BYTES_IN_PAGE;

    double footprintOverMaxHeapSize = ((double) committedBytes) / ((double) maxHeapSize);
    double footprintOverUsedHeapSize = ((double) committedBytes) / ((double) usedHeapSize);
    double utilization = ((double) recordedCards) / ((double) (committedBytes));

    remsetFootprintOverMaxHeapSize.inc(footprintOverMaxHeapSize);
    remsetFootprintOverUsedHeapSize.inc(footprintOverUsedHeapSize);
    remsetUtilization.inc(utilization);
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
  @Interruptible
  public void processOptions() {
    super.processOptions();
    {
      int oldPhase= Phase.schedulePlaceholder(STAT_REMSET);
      int newPhase = Phase.scheduleComplex(statRemSet);
      ComplexPhase cp = (ComplexPhase) Phase.getPhase(mixedCollection);
      cp.replacePhase(oldPhase, newPhase);
    }
    {
      int oldPhase= Phase.schedulePlaceholder(STAT_REMSET);
      int newPhase = Phase.scheduleComplex(statRemSet);
      ComplexPhase cp = (ComplexPhase) Phase.getPhase(nurseryCollection);
      cp.replacePhase(oldPhase, newPhase);
    }
  }
}
