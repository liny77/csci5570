#include "driver/engine.hpp"

#include <vector>

#include "base/abstract_partition_manager.hpp"
#include "base/node.hpp"
#include "comm/mailbox.hpp"
#include "comm/sender.hpp"
#include "driver/ml_task.hpp"
#include "driver/simple_id_mapper.hpp"
#include "driver/worker_spec.hpp"
#include "server/server_thread.hpp"
#include "worker/abstract_callback_runner.hpp"
#include "worker/worker_thread.hpp"

namespace csci5570 {

void Engine::StartEverything(int num_server_threads_per_node) {
  CreateIdMapper(num_server_threads_per_node);
  CreateMailbox();
  StartSender();
  StartServerThreads();
  StartWorkerThreads();
  StartMailbox();
}
void Engine::CreateIdMapper(int num_server_threads_per_node) {
  id_mapper_.reset(new SimpleIdMapper(node_, nodes_));
  id_mapper_->Init(num_server_threads_per_node);
}
void Engine::CreateMailbox() {
  const auto node_const = node_;
  const auto nodes_const = nodes_;
  mailbox_.reset(new Mailbox(node_const, nodes_const, id_mapper_.get()));
}
void Engine::StartServerThreads() {
  auto tids = id_mapper_->GetServerThreadsForId(node_.id);
  server_thread_group_.reserve(tids.size());
  for (auto tid : tids) {
    auto server_thread = std::unique_ptr<ServerThread>(new ServerThread(tid));
    mailbox_->RegisterQueue(tid, server_thread->GetWorkQueue());
    server_thread->Start();
    server_thread_group_.push_back(std::move(server_thread));
  }
}
void Engine::StartWorkerThreads() {
  callback_runner_.reset(new CallbackRunner());
  // only one worker helper thread
  worker_thread_.reset(new WorkerHelperThread(id_mapper_->GetWorkerHelperThreadsForId(node_.id)[0], 
                                              callback_runner_.get()));
  worker_thread_->Start();
}
void Engine::StartMailbox() {
  mailbox_->Start();
}
void Engine::StartSender() {
  sender_.reset(new Sender(mailbox_.get()));
  sender_->Start();
}

void Engine::StopEverything() {
  StopSender();
  StopMailbox();
  StopServerThreads();
  StopWorkerThreads();
}
void Engine::StopServerThreads() {
  Message msg;
  msg.meta.flag = Flag::kExit;
  for (int i = 0; i < server_thread_group_.size(); ++i) {
    msg.meta.recver = server_thread_group_[i]->GetId();
    server_thread_group_[i]->GetWorkQueue()->Push(msg);
    server_thread_group_[i]->Stop();
  }
  server_thread_group_.clear();
}
void Engine::StopWorkerThreads() {
  Message msg;
  msg.meta.recver = worker_thread_->GetId();
  msg.meta.flag = Flag::kExit;

  worker_thread_->GetWorkQueue()->Push(msg);
  worker_thread_->Stop();
}
void Engine::StopSender() {
  sender_->Stop();
}
void Engine::StopMailbox() {
  Barrier();
  mailbox_->Stop();
}

void Engine::Barrier() {
  mailbox_->Barrier();
}

WorkerSpec Engine::AllocateWorkers(const std::vector<WorkerAlloc>& worker_alloc) {
  WorkerSpec worker_spec(worker_alloc);
  for (auto alloc : worker_alloc) {
    auto worker_ids = worker_spec.GetLocalWorkers(alloc.node_id);
    for (int i = 0; i < alloc.num_workers; ++i) {
      auto thread_id = id_mapper_->AllocateWorkerThread(alloc.node_id);
      //thread_id may be -1 --- unsuccess
      worker_spec.InsertWorkerIdThreadId(worker_ids[i], thread_id);
    }
  }
  return worker_spec;
}

void Engine::InitTable(uint32_t table_id, const std::vector<uint32_t>& worker_ids) {
  ThreadsafeQueue<Message> queue;
  auto init_worker_tid = id_mapper_->AllocateWorkerThread(node_.id);
  mailbox_->RegisterQueue(init_worker_tid, &queue);
  Message init_msg;
  init_msg.meta.flag = Flag::kResetWorkerInModel;
  init_msg.meta.sender = init_worker_tid;
  init_msg.meta.model_id = table_id;
  init_msg.AddData(third_party::SArray<uint32_t>(worker_ids));
  auto server_ids = id_mapper_->GetAllServerThreads();
  for (auto s_id : server_ids) {
    init_msg.meta.recver = s_id;
    sender_->GetMessageQueue()->Push(init_msg);
  }
  while (queue.Size() != server_ids.size());
  
  mailbox_->unregisterQueue(init_worker_tid);
  id_mapper_->DeallocateWorkerThread(node_.id, init_worker_tid);
}

void Engine::Run(const MLTask& task) {
  if (!task.IsSetup()) return;
  auto worker_spec = AllocateWorkers(task.GetWorkerAlloc());
  auto local_worker_tids = worker_spec.GetLocalThreads(node_.id);
  auto tables = task.GetTables();
  for (auto table : tables) {
    InitTable(table, local_worker_tids);
  }
  auto worker_tid2worker_id = worker_spec.GetThreadToWorker();
  for (auto tid : local_worker_tids) {
    Info info;
    info.send_queue = sender_->GetMessageQueue();
    info.thread_id = tid;
    info.worker_id = worker_tid2worker_id[tid];
    info.callback_runner = callback_runner_.get();
    for (auto it = partition_manager_map_.begin(); it != partition_manager_map_.end(); ++it) {
      info.partition_manager_map[it->first] = it->second.get();
    }
    // use user thread id, and worker helper thread's queue
    mailbox_->RegisterQueue(tid, worker_thread_->GetWorkQueue());
    std::thread udf_thread([&task, info] { task.RunLambda(info); });
    udf_thread.join();
  }
}

void Engine::RegisterPartitionManager(uint32_t table_id, std::unique_ptr<AbstractPartitionManager> partition_manager) {
  partition_manager_map_[table_id] = std::move(partition_manager);
}

}  // namespace csci5570
