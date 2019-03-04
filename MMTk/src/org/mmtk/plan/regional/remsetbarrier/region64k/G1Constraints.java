package org.mmtk.plan.regional.remsetbarrier.region64k;

import org.vmmagic.pragma.Uninterruptible;

@Uninterruptible
public class G1Constraints extends org.mmtk.plan.regional.remsetbarrier.G1Constraints {
    @Override
    public int LOG_PAGES_IN_G1_REGION() {
        return 4;
    }
}
