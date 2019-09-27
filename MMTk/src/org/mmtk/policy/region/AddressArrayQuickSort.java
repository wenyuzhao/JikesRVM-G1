package org.mmtk.policy.region;

import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.NoInline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.AddressArray;


@Uninterruptible
public class AddressArrayQuickSort {
  @Inline
  public static void sort(AddressArray regions, AddressArray regionSizes) {
    sort(regions, regionSizes, 0, regions.length() - 1);
  }

  @NoInline
  private static void sort(AddressArray regions, AddressArray regionSizes, int lo, int hi) {
    if (hi <= lo) return;
    int j = partition(regions, regionSizes, lo, hi);
    sort(regions, regionSizes, lo, j-1);
    sort(regions, regionSizes, j+1, hi);
  }

  @Inline
  private static int partition(AddressArray regions, AddressArray regionSizes, int lo, int hi) {
    int i = lo, j = hi + 1;
    int size = regionSizes.get(lo).toInt();
    while (true) {
      while (regionSizes.get(++i).toInt() < size)
        if (i == hi) break;
      while (size < regionSizes.get(--j).toInt())
        if (j == lo) break;

      if (i >= j) break;
      swap(regions, regionSizes, i, j);
    }
    swap(regions, regionSizes, lo, j);
    return j;
  }

  @Inline
  private static void swap(AddressArray regions, AddressArray regionSizes, int i, int j) {
    Address temp;
    // regions
    temp = regions.get(i);
    regions.set(i, regions.get(j));
    regions.set(j, temp);
    // regionSizes
    temp = regionSizes.get(i);
    regionSizes.set(i, regionSizes.get(j));
    regionSizes.set(j, temp);
  }
}

