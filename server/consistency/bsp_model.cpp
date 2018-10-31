#include "server/consistency/bsp_model.hpp"
#include "glog/logging.h"

namespace csci5570 {

BSPModel::BSPModel(uint32_t model_id, std::unique_ptr<AbstractStorage>&& storage_ptr,
                   ThreadsafeQueue<Message>* reply_queue):model_id_(model_id), storage_(std::move(storage_ptr)),
                                                        reply_queue_(reply_queue) {}

void BSPModel::Clock(Message& msg) {
  if (!progress_tracker_.CheckThreadValid(msg.meta.sender)) return;
  if (GetProgress(msg.meta.sender) > progress_tracker_.GetMinClock()) return;//thread which is ahead should not clock

  int cur_mini_clock = progress_tracker_.AdvanceAndGetChangedMinClock(msg.meta.sender);
  if (cur_mini_clock != -1) {// min_clock changed, process all buffered msg
    for (auto pendingMsg : add_buffer_) {
      storage_->Add(pendingMsg);
    }
    for (auto pendingMsg : get_buffer_) {
      reply_queue_->Push(storage_->Get(pendingMsg));
    }
    add_buffer_.clear();
    get_buffer_.clear();
  }
}

void BSPModel::Add(Message& msg) {
  if (!progress_tracker_.CheckThreadValid(msg.meta.sender)) return;
  add_buffer_.push_back(msg);// collect all the add msgs, they are processed when the min_clock advances
}

void BSPModel::Get(Message& msg) {
  if (!progress_tracker_.CheckThreadValid(msg.meta.sender)) return;
  // if current thread's clock is ahead, its get msgs are buffered
  if (progress_tracker_.GetMinClock() < GetProgress(msg.meta.sender)) get_buffer_.push_back(msg);
  else reply_queue_->Push(storage_->Get(msg));
}

int BSPModel::GetProgress(int tid) {
  return progress_tracker_.GetProgress(tid);
}

int BSPModel::GetGetPendingSize() {
  return get_buffer_.size();
}

int BSPModel::GetAddPendingSize() {
  return add_buffer_.size();
}

void BSPModel::ResetWorker(Message& msg) {
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
