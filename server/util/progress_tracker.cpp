#include "server/util/progress_tracker.hpp"

#include "glog/logging.h"

namespace csci5570 {

void ProgressTracker::Init(const std::vector<uint32_t>& tids) {
  for (auto tid : tids) {
    progresses_[tid] = 0;
  }
  min_clock_ = 0;
}

int ProgressTracker::AdvanceAndGetChangedMinClock(int tid) {
  int rst;
  if (IsUniqueMin(tid)) {
    min_clock_++;
    rst = min_clock_;
  } else {
    rst = -1;
  }
  progresses_[tid]++;
  return rst;
}

int ProgressTracker::GetNumThreads() const {
  return progresses_.size();
}

int ProgressTracker::GetProgress(int tid) const {
  return CheckThreadValid(tid) ? progresses_.find(tid)->second : -1;
}

int ProgressTracker::GetMinClock() const {
  return min_clock_;
}

bool ProgressTracker::IsUniqueMin(int tid) const {
  if (GetProgress(tid) != min_clock_) return false;
  for (auto pair : progresses_) {
    if (pair.second == min_clock_ && pair.first != tid) {
      return false;
    }
  }
  return true;
}

bool ProgressTracker::CheckThreadValid(int tid) const {
  return progresses_.find(tid) != progresses_.end();
}

}  // namespace csci5570
