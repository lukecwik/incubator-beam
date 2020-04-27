package org.apache.beam.sdk.transforms.splittabledofn;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.splittabledofn.ClaimSetTracker.ClaimSet;

public class ClaimSetTracker<T> extends RestrictionTracker<ClaimSet<T>, T> {

  private final Poller<T> poller;
  private final GarbageCollector<T> garbageCollector;
  private final Map<Object, T> initiallyClaimed;
  private final Map<Object, T> initiallyUnclaimed;
  private final Map<Object, T> newlyClaimed;
  private final Coder<T> coder;

  public ClaimSetTracker(Poller<T> poller, GarbageCollector<T> garbageCollector, ClaimSet<T> currentRestriction, Coder<T> coder) {
    this.poller = poller;
    this.garbageCollector = garbageCollector;
    this.initiallyClaimed = new HashMap<>();
    this.initiallyUnclaimed = new HashMap<>();
    this.newlyClaimed = new HashMap<>();
    this.coder = coder;
  }

  public interface Poller<T> {
    ClaimSet<T> poll(ClaimSet<T> claimSet);
  }

  public interface GarbageCollector<T> {
    ClaimSet<T> garbageCollect(ClaimSet<T> claimSet);
  }



  @Override
  public boolean tryClaim(T position) {

    return false;
  }

  @Override
  public ClaimSet<T> currentRestriction() {
    return null;
  }

  @Nullable
  @Override
  public SplitResult<Set<T>> trySplit(double fractionOfRemainder) {
    return null;
  }

  @Override
  public void checkDone() throws IllegalStateException {
    checkState(currentRestriction.getUnclaimed().containsAll(newlyClaimed));
  }

  @AutoValue
  public abstract static class ClaimSet<T> {
    public static <T> ClaimSet<T> from(Set<T> claimed, Set<T> unclaimed) {
      return new AutoValue_ClaimSetTracker_ClaimSet<>(claimed, unclaimed);
    }

    public abstract Set<T> getClaimed();
    public abstract Set<T> getUnclaimed();
  }


}
