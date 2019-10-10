package org.mmtk.policy.region;

import org.mmtk.plan.g1.G1;
import org.mmtk.plan.g1.PauseTimePredictor;
import org.mmtk.utility.Constants;
import org.mmtk.utility.Log;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.NoInline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.Word;
import org.vmmagic.unboxed.WordArray;


@Uninterruptible
public class CollectionSet {
  @Uninterruptible
  public static abstract class Predictor {
    public abstract void record(Address region);
    public abstract boolean withinBudget();
  }


  public static void compute(RegionSpace space, int gcKind, int availablePages, PauseTimePredictor predictor) {
    availablePages = ((int) ((availablePages >>> Region.LOG_PAGES_IN_REGION) * Region.MEMORY_RATIO) << Region.LOG_PAGES_IN_REGION);
    switch (gcKind) {
      case G1.GCKind.YOUNG: computeForNurseryGC(space, availablePages); return;
      case G1.GCKind.MIXED: computeForMixedGC(space, availablePages, predictor);   return;
      case G1.GCKind.FULL:  computeForFullGC(space, availablePages);    return;
    }
  }

  private static void computeForNurseryGC(RegionSpace space, int availablePages) {
    for (Address region = space.firstRegion(); !region.isZero(); region = Region.getNext(region)) {
      if (Region.getInt(region, Region.MD_GENERATION) != Region.OLD) {
        markAsRelocate(region, Region.liveBytes(region));
      }
    }
  }

  private static void computeForMixedGC(RegionSpace space, final int availablePages, final PauseTimePredictor predictor) {
    final int regions = space.committedRegions;
    WordArray array = createArrayOfRegionsAndSizes(regions, space.firstRegion());
    int availableBytes = availablePages << Constants.LOG_BYTES_IN_PAGE;

    // Select all nursery regions
    for (int i = 0; i < regions; i++) {
      Address region = getRegion(array, i);
      if (Region.getInt(region, Region.MD_GENERATION) != Region.OLD) {
        int size = getSize(array, i);
        markAsRelocate(region, size);
        availableBytes -= size;
        predictor.predict(region, true);
      }
    }
    // Select some old regions
    if (availableBytes <= 0) return;
    int count = 0;
    for (int i = 0; i < regions; i++) {
      Address region = getRegion(array, i);
      if (Region.getInt(region, Region.MD_GENERATION) != Region.OLD) continue;
      int size = getSize(array, i);
      if (availableBytes - size <= 0) break;
      if (predictor.predict(region, false)) {
        availableBytes -= size;
        markAsRelocate(region, size);
        count ++;
      } else {
        break;
      }
    }
  }

  private static void computeForFullGC(RegionSpace space, int availablePages) {
    final int regions = space.committedRegions;
    WordArray array = createArrayOfRegionsAndSizes(regions, space.firstRegion());
    int availableBytes = availablePages << Constants.LOG_BYTES_IN_PAGE;
    // Select all nursery regions
    for (int i = 0; i < regions; i++) {
      Address region = getRegion(array, i);
      if (Region.getInt(region, Region.MD_GENERATION) != Region.OLD) {
        int size = getSize(array, i);
        markAsRelocate(region, size);
        availableBytes -= size;
      }
    }
    // Select some old regions
    if (availableBytes <= 0) return;
    for (int i = 0; i < regions; i++) {
      Address region = getRegion(array, i);
      if (Region.getInt(region, Region.MD_GENERATION) != Region.OLD) continue;
      int size = getSize(array, i);
      availableBytes -= size;
      if (availableBytes <= 0) break;
      markAsRelocate(region, size);
    }
  }

  @Inline
  private static void markAsRelocate(Address region, int size) {
    if (Region.VERBOSE_REGION_LIFETIME) {
      Log.write("Relocate ");
      Log.write(Region.getGenerationName(region));
      Log.writeln(" region ", region);
    }
    G1.predictor.stat.totalCopyBytes += size;
    Region.set(region, Region.MD_RELOCATE, true);
  }

  private static WordArray createArrayOfRegionsAndSizes(int regions, Address headRegion) {
    // Initialize: Array<(Region, Size)>
    WordArray array = WordArray.create(regions << 1);
    int cursor = 0;
    for (Address region = headRegion; !region.isZero(); region = Region.getNext(region)) {
      array.set(cursor, region.toWord());
      array.set(cursor + 1, Word.fromIntZeroExtend(Region.liveBytes(region)));
      cursor += 2;
    }
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(cursor == regions * 2);
    // Sort in ascending order
    sort(array, regions);
    return array;
  }

  @Inline
  private static void sort(WordArray array, int count) {
    sort(array, 0, count - 1);
  }

  @NoInline
  private static void sort(WordArray array, int lo, int hi) {
    if (hi <= lo) return;
    int j = partition(array, lo, hi);
    sort(array, lo, j - 1);
    sort(array, j + 1, hi);
  }

  @Inline
  private static int partition(WordArray array, int lo, int hi) {
    int i = lo, j = hi + 1;
    int size = getSize(array, lo);
    while (true) {
      while (getSize(array, ++i) < size)
        if (i == hi) break;
      while (size < getSize(array, --j))
        if (j == lo) break;

      if (i >= j) break;
      swap(array, i, j);
    }
    swap(array, lo, j);
    return j;
  }

  @Inline
  private static int getSize(WordArray array, int i) {
    return array.get((i << 1) + 1).toInt();
  }

  @Inline
  private static Address getRegion(WordArray array, int i) {
    Address region = array.get((i << 1) + 0).toAddress();
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(G1.regionSpace.contains(region));
    return region;
  }

  @Inline
  private static void swap(WordArray array, int i, int j) {
    Word temp0, temp1;
    // regions
    temp0 = array.get((i << 1) + 0);
    temp1 = array.get((i << 1) + 1);
    array.set((i << 1) + 0, array.get((j << 1) + 0));
    array.set((i << 1) + 1, array.get((j << 1) + 1));
    array.set((j << 1) + 0, temp0);
    array.set((j << 1) + 1, temp1);
  }
}

