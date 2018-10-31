#include "server/consistency/asp_model.hpp"
#include "glog/logging.h"

namespace csci5570 {

ASPModel::ASPModel(uint32_t model_id, std::unique_ptr<AbstractStorage>&& storage_ptr,
                   ThreadsafeQueue<Message>* reply_queue): model_id_(model_id), storage_(std::move(storage_ptr)),
                                                          reply_queue_(reply_queue) {}

// sender is a thread id?
void ASPModel::Clock(Message& msg) {
  if (progress_tracker_.CheckThreadValid(msg.meta.sender))
    progress_tracker_.AdvanceAndGetChangedMinClock(msg.meta.sender);
}

void ASPModel::Add(Message& msg) {
  if (progress_tracker_.CheckThreadValid(msg.meta.sender))
    storage_->Add(msg);
}

void ASPModel::Get(Message& msg) {
  if (progress_tracker_.CheckThreadValid(msg.meta.sender))
    reply_queue_->Push(storage_->Get(msg));
}

int ASPModel::GetProgress(int tid) {
  return progress_tracker_.GetProgress(tid);
}

void ASPModel::ResetWorker(Message& msg) {
  // convert char[] to uint32_t !!!!!!
  third_party::SArray<uint32_t> tids(msg.data[0]);
  progress_tracker_.Init(std::vector<uint32_t>(tids.begin(), tids.end()));
  Message reply;
  reply.meta.flag = Flag::kResetWorkerInModel;
  reply.meta.model_id = msg.meta.model_id;
  reply.meta.sender = msg.meta.recver;
  reply.meta.recver = msg.meta.sender;

  reply_queue_->Push(reply);
}

}  // namespace csci5570
