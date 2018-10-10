#pragma once

#include "base/abstract_partition_manager.hpp"
#include "base/magic.hpp"
#include "base/message.hpp"
#include "base/third_party/sarray.h"
#include "base/threadsafe_queue.hpp"
#include "worker/abstract_callback_runner.hpp"

#include <cinttypes>
#include <vector>

namespace csci5570 {

/**
 * Provides the API to users, and implements the worker-side abstraction of model
 * Each model in one application is uniquely handled by one KVClientTable
 *
 * @param Val type of model parameter values
 */
template <typename Val>
class KVClientTable {
 public:
  /**
   * @param app_thread_id       user thread id
   * @param model_id            model id
   * @param sender_queue        the work queue of a sender communication thread
   * @param partition_manager   model partition manager
   * @param callback_runner     callback runner to handle received replies from servers
   */
  KVClientTable(uint32_t app_thread_id, uint32_t model_id, ThreadsafeQueue<Message>* const sender_queue,
                const AbstractPartitionManager* const partition_manager, AbstractCallbackRunner* const callback_runner)
      : app_thread_id_(app_thread_id),
        model_id_(model_id),
        sender_queue_(sender_queue),
        partition_manager_(partition_manager),
        callback_runner_(callback_runner){};

  // ========== API ========== //
  void Clock();
  // vector version
  void Add(const std::vector<Key>& keys, const std::vector<Val>& vals) {
    Add(third_party::SArray<Key>(keys), third_party::SArray<Val>(vals));
  }
  void Get(const std::vector<Key>& keys, std::vector<Val>* vals) {
    std::vector<std::pair<int, AbstractPartitionManager::Keys> > sliced;
    partition_manager_->Slice(AbstractPartitionManager::Keys(keys), &sliced);
    callback_runner_->RegisterRecvHandle(app_thread_id_, model_id_,
      [vals](Message &msg) {
        third_party::SArray<Val> temp(msg.data[1]);
        int origin_size = vals->size();
        vals->resize(origin_size + temp.size());
        std::copy(temp.begin(), temp.end(), vals->begin() + origin_size);
      });
    callback_runner_->RegisterRecvFinishHandle(app_thread_id_, model_id_, []{});
    for (auto piece : sliced) {
      Message msg;
      msg.meta.sender = app_thread_id_;
      msg.meta.recver = piece.first;
      msg.meta.model_id = model_id_;
      msg.meta.flag = Flag::kGet;
      msg.AddData(piece.second);
      sender_queue_->Push(msg);
    }
    callback_runner_->NewRequest(app_thread_id_, model_id_, sliced.size());
    callback_runner_->WaitRequest(app_thread_id_, model_id_);
  }
  // sarray version
  void Add(const third_party::SArray<Key>& keys, const third_party::SArray<Val>& vals) {
    std::vector<std::pair<int,AbstractPartitionManager::KVPairs> > sliced;
    partition_manager_->Slice(std::make_pair(keys, vals), &sliced);
    for (auto piece : sliced) {
      Message msg;
      msg.meta.sender = app_thread_id_;
      msg.meta.recver = piece.first;
      msg.meta.model_id = model_id_;
      msg.meta.flag = Flag::kAdd;
      msg.AddData(piece.second.first);
      msg.AddData(piece.second.second);
      sender_queue_->Push(msg);
    }
  }
  void Get(const third_party::SArray<Key>& keys, third_party::SArray<Val>* vals) {
    std::vector<std::pair<int, AbstractPartitionManager::Keys> > sliced;
    partition_manager_->Slice(keys, &sliced);
    callback_runner_->RegisterRecvHandle(app_thread_id_, model_id_,
      [vals](Message &msg) {
        third_party::SArray<Val> temp(msg.data[1]);
        vals->append(temp);
      });
    callback_runner_->RegisterRecvFinishHandle(app_thread_id_, model_id_, []{});
    for (auto piece : sliced) {
      Message msg;
      msg.meta.sender = app_thread_id_;
      msg.meta.recver = piece.first;
      msg.meta.model_id = model_id_;
      msg.meta.flag = Flag::kGet;
      msg.AddData(piece.second);
      sender_queue_->Push(msg);
    }
    callback_runner_->NewRequest(app_thread_id_, model_id_, sliced.size());
    callback_runner_->WaitRequest(app_thread_id_, model_id_);
  }
  // ========== API ========== //

 private:
  uint32_t app_thread_id_;  // identifies the user thread
  uint32_t model_id_;       // identifies the model on servers

  ThreadsafeQueue<Message>* const sender_queue_;             // not owned
  AbstractCallbackRunner* const callback_runner_;            // not owned
  const AbstractPartitionManager* const partition_manager_;  // not owned

};  // class KVClientTable

}  // namespace csci5570
