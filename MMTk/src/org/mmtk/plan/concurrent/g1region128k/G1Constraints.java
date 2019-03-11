package org.mmtk.plan.concurrent.g1region128k;

import org.vmmagic.pragma.Uninterruptible;

/**
 * SemiSpace common constants.
 */
@Uninterruptible
public class G1Constraints extends org.mmtk.plan.concurrent.g1.G1Constraints {
  @Override
  public int LOG_PAGES_IN_G1_REGION() {
    return 5;
  }
}
