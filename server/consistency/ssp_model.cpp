#include "server/consistency/ssp_model.hpp"
#include "glog/logging.h"

namespace csci5570 {

SSPModel::SSPModel(uint32_t model_id, std::unique_ptr<AbstractStorage>&& storage_ptr, int staleness,
                   ThreadsafeQueue<Message>* reply_queue): model_id_(model_id), storage_(std::move(storage_ptr)),
                                                        staleness_(staleness), reply_queue_(reply_queue) {}
void SSPModel::Clock(Message& msg) {
  if (!progress_tracker_.CheckThreadValid(msg.meta.sender)) return;
  int cur_mini_clock = progress_tracker_.AdvanceAndGetChangedMinClock(msg.meta.sender);
  if (cur_mini_clock != -1 && GetPendingSize(cur_mini_clock) > 0) {// min_clock changed, process pending messages if needed
    auto pendingMsgs = buffer_.Pop(cur_mini_clock);
    for (auto pending : pendingMsgs) {
      if (pending.meta.flag == Flag::kAdd) Add(pending);
      if (pending.meta.flag == Flag::kGet) Get(pending);
    }
  }
}

// suppose current clock 3, min clock 0, staleness 2
// should wait for min clock 1 (3 - 2)
void SSPModel::Add(Message& msg) {
  if (!progress_tracker_.CheckThreadValid(msg.meta.sender)) return;
  auto cur_clock = GetProgress(msg.meta.sender);
  if (cur_clock - progress_tracker_.GetMinClock() <= staleness_) {
    storage_->Add(msg);
  } else {
    buffer_.Push(cur_clock - staleness_, msg);
  }
}

void SSPModel::Get(Message& msg) {
  if (!progress_tracker_.CheckThreadValid(msg.meta.sender)) return;
  auto cur_clock = GetProgress(msg.meta.sender);
  if (cur_clock - progress_tracker_.GetMinClock() <= staleness_) {
    reply_queue_->Push(storage_->Get(msg));
  } else {
    buffer_.Push(cur_clock - staleness_, msg);
  }
}

int SSPModel::GetProgress(int tid) {
  return progress_tracker_.GetProgress(tid);
}

int SSPModel::GetPendingSize(int progress) {
  return buffer_.Size(progress);
}

void SSPModel::ResetWorker(Message& msg) {
  // convert char[] to uint32_t !!!!!!
  third_party::SArray<uint32_t> tids(msg.data[0]);
  progress_tracker_.Init(std::vector<uint32_t>(tids.begin(), tids.end()));
  Message relpy;
  relpy.meta.flag = Flag::kResetWorkerInModel;
  relpy.meta.model_id = msg.meta.model_id;
  relpy.meta.recver = msg.meta.sender;
  relpy.meta.sender = msg.meta.recver;
  
  reply_queue_->Push(relpy);
}

}  // namespace csci5570
