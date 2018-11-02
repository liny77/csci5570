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
  // id mapper
  
  // mailbox
  
  // sender
  
  // serverthreads, workerthreads
  
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

}
void Engine::StartWorkerThreads() {
  // TODO implement worker thread
}
void Engine::StartMailbox() {
  mailbox_->Start();
}
void Engine::StartSender() {
  sender_.reset(new Sender(mailbox_.get()));
  sender_->Start();
}

void Engine::StopEverything() {
  // TODO
}
void Engine::StopServerThreads() {
  // TODO
}
void Engine::StopWorkerThreads() {
  // TODO
}
void Engine::StopSender() {
  // TODO
}
void Engine::StopMailbox() {
  // TODO
}

void Engine::Barrier() {
  // TODO
}

WorkerSpec Engine::AllocateWorkers(const std::vector<WorkerAlloc>& worker_alloc) {
  // TODO
}

void Engine::InitTable(uint32_t table_id, const std::vector<uint32_t>& worker_ids) {
  // TODO
}

void Engine::Run(const MLTask& task) {
  // TODO
}

void Engine::RegisterPartitionManager(uint32_t table_id, std::unique_ptr<AbstractPartitionManager> partition_manager) {
  // TODO
}

}  // namespace csci5570
