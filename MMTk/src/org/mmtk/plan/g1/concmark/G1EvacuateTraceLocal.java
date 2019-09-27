package org.mmtk.plan.g1.concmark;

import org.mmtk.plan.Trace;
import org.vmmagic.pragma.Uninterruptible;

@Uninterruptible
public class G1EvacuateTraceLocal extends org.mmtk.plan.g1.baseline.G1EvacuateTraceLocal {
  public G1EvacuateTraceLocal(Trace trace) {
    super(trace);
  }
}
