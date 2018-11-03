#include "driver/worker_spec.hpp"
#include "glog/logging.h"

namespace csci5570 {

WorkerSpec::WorkerSpec(const std::vector<WorkerAlloc>& worker_alloc) {
  Init(worker_alloc);
}
bool WorkerSpec::HasLocalWorkers(uint32_t node_id) const {
  auto it = node_to_workers_.find(node_id);
  return it != node_to_workers_.end() && !it->second.empty();
}
const std::vector<uint32_t>& WorkerSpec::GetLocalWorkers(uint32_t node_id) const {
  return node_to_workers_.at(node_id);// not found will throw exception
}
const std::vector<uint32_t>& WorkerSpec::GetLocalThreads(uint32_t node_id) const {
  return node_to_threads_.at(node_id);// not found will throw exception
}

std::map<uint32_t, std::vector<uint32_t>> WorkerSpec::GetNodeToWorkers() {
  return node_to_workers_;
}

std::vector<uint32_t> WorkerSpec::GetAllThreadIds() {
  return std::vector<uint32_t>(thread_ids_.begin(), thread_ids_.end());
}

void WorkerSpec::InsertWorkerIdThreadId(uint32_t worker_id, uint32_t thread_id) {
  worker_to_thread_[worker_id] = thread_id;
  thread_to_worker_[thread_id] = worker_id;
  auto node_id = worker_to_node_[worker_id];
  auto found = node_to_threads_.find(node_id);
  if (found == node_to_threads_.end()) {
    node_to_threads_[node_id] = std::vector<uint32_t>(1, thread_id);
  } else {
    found->second.push_back(thread_id);
  }
  thread_ids_.insert(thread_id);
}

void WorkerSpec::Init(const std::vector<WorkerAlloc>& worker_alloc) {
  std::vector<uint32_t> workers;
  for (auto alloc : worker_alloc) {
    workers.resize(alloc.num_workers);
    for (int i = 0; i < workers.size(); ++i) {
      worker_to_node_[num_workers_] = alloc.node_id;
      workers[i] = num_workers_;
      num_workers_++;
    }
    node_to_workers_[alloc.node_id] = workers;
    workers.clear();
  }
}
}  // namespace csci5570
