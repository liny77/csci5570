#pragma once

#include <functional>

#include "base/message.hpp"

namespace csci5570 {

class AbstractCallbackRunner {
 public:
  /**
   * Register callbacks for receiving a message
   */
  virtual void RegisterRecvHandle(uint32_t app_thread_id, uint32_t model_id,
                                  const std::function<void(Message&)>& recv_handle) = 0;
  /**
   * Register callbacks for when all expected responses are received
   */
  virtual void RegisterRecvFinishHandle(uint32_t app_thread_id, uint32_t model_id,
                                        const std::function<void()>& recv_finish_handle) = 0;

  /**
   * Register a new request which expects to receive <expected_responses> responses
   */
  virtual void NewRequest(uint32_t app_thread_id, uint32_t model_id, uint32_t expected_responses) = 0;

  /**
   * Return when the request is completed
   */
  virtual void WaitRequest(uint32_t app_thread_id, uint32_t model_id) = 0;

  /**
   * Used by the worker threads on receival of messages and to invoke callbacks
   */
  virtual void AddResponse(uint32_t app_thread_id, uint32_t model_id, Message& msg) = 0;
};  // class AbstractCallbackRunner

class CallbackRunner: public AbstractCallbackRunner {
  public:
    CallbackRunner() {}
    void RegisterRecvHandle(uint32_t app_thread_id, uint32_t model_id,
                            const std::function<void(Message&)>& recv_handle) {
      recv_handles_[app_thread_id][model_id] = recv_handle;
    }
    void RegisterRecvFinishHandle(uint32_t app_thread_id, uint32_t model_id,
                                        const std::function<void()>& recv_finish_handle) {
      recv_finish_handles_[app_thread_id][model_id] = recv_finish_handle;
    }
    void NewRequest(uint32_t app_thread_id, uint32_t model_id, uint32_t expected_responses) {
      trackers_[app_thread_id][model_id] = {expected_responses, 0};
    }
    void WaitRequest(uint32_t app_thread_id, uint32_t model_id) {
      std::unique_lock<std::mutex> lk(mu_);
      cond_.wait(lk, [this, app_thread_id, model_id] {
          auto tracker = trackers_[app_thread_id][model_id];
          return tracker.first == tracker.second;
        });
    }
    void AddResponse(uint32_t app_thread_id, uint32_t model_id, Message& msg) {
      bool recv_finish = false;
      auto &tracker = trackers_[app_thread_id][model_id];
      {
        std::lock_guard<std::mutex> lk(mu_);
        recv_finish = tracker.first == tracker.second + 1 ? true : false;
      }

      if (msg.meta.flag == Flag::kGet) {
        recv_handles_[app_thread_id][model_id](msg);
      } else if (msg.meta.flag == Flag::kResetWorkerInModel) printf("kResetWorkerInModel\n");

      if (recv_finish) {
        if (msg.meta.flag == Flag::kGet) recv_finish_handles_[app_thread_id][model_id]();
      }
      {
        std::lock_guard<std::mutex> lk(mu_);
        tracker.second += 1;
        if (recv_finish) {
          cond_.notify_all();
        }
      }
    }
  private:
    std::mutex mu_;// lockable obj
    std::condition_variable cond_;
    // app_thread_id(user thread)   model_id
    std::map<uint32_t, std::map<uint32_t, std::function<void(Message&)>>> recv_handles_;
    std::map<uint32_t, std::map<uint32_t, std::function<void()>>> recv_finish_handles_;
    std::map<uint32_t, std::map<uint32_t, std::pair<uint32_t, uint32_t>>> trackers_;
};

}  // namespace csci5570
