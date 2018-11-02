#include "driver/simple_id_mapper.hpp"

#include <cinttypes>
#include <vector>

#include "base/node.hpp"

namespace csci5570 {

SimpleIdMapper::SimpleIdMapper(Node node, const std::vector<Node>& nodes) {
  node_ = node;
  nodes_.assign(nodes.begin(), nodes.end());
}

uint32_t SimpleIdMapper::GetNodeIdForThread(uint32_t tid) {
  return tid / kMaxThreadsPerNode;
}

//  i...[server]...kworkerHelper...[worker_helper]...kMaxbgThread...[user_worker]...i+kMaxThreadPerNode
void SimpleIdMapper::Init(int num_server_threads_per_node) {
  if (num_server_threads_per_node < 1 || num_server_threads_per_node >= kWorkerHelperThreadId) return;
  std::vector<uint32_t> server_tids;
  std::vector<uint32_t> worker_helper_tids;
  server_tids.resize(num_server_threads_per_node);
  // worker_helper_tids.resize(kMaxBgThreadsPerNode - kWorkerHelperThreadId);
  
  // for simplication, only one worker thread on each process for now
  worker_helper_tids.resize(1);

  for (auto node : nodes_) {
    for (uint32_t i = 0; i < server_tids.size(); ++i) {
      server_tids[i] = node.id * kMaxThreadsPerNode + i;
    }
    for (uint32_t i = 0; i < worker_helper_tids.size(); ++i) {
      worker_helper_tids[i] = node.id * kMaxThreadsPerNode + kWorkerHelperThreadId + i;
    }
    node2server_[node.id] = server_tids;
    node2worker_helper_[node.id] = worker_helper_tids;
  }
}

uint32_t SimpleIdMapper::AllocateWorkerThread(uint32_t node_id) {
  auto &worker_tid_set = node2worker_[node_id];
  if (worker_tid_set.size() >= kMaxThreadsPerNode - kMaxBgThreadsPerNode) return -1;
  for (int i = node_id * kMaxThreadsPerNode + kMaxBgThreadsPerNode; i < (node_id + 1) * kMaxThreadsPerNode; ++i) {
    if (worker_tid_set.find(i) == worker_tid_set.end()) {
      worker_tid_set.insert(i);
      return i;
    }
  }
}
void SimpleIdMapper::DeallocateWorkerThread(uint32_t node_id, uint32_t tid) {
  node2worker_[node_id].erase(tid);
}

std::vector<uint32_t> SimpleIdMapper::GetServerThreadsForId(uint32_t node_id) {
  return node2server_[node_id];
}
std::vector<uint32_t> SimpleIdMapper::GetWorkerHelperThreadsForId(uint32_t node_id) {
  return node2worker_helper_[node_id];
}
std::vector<uint32_t> SimpleIdMapper::GetWorkerThreadsForId(uint32_t node_id) {
  auto woker_set = node2worker_[node_id];
  return std::vector<uint32_t>(woker_set.begin(), woker_set.end());
}
std::vector<uint32_t> SimpleIdMapper::GetAllServerThreads() {
  std::vector<uint32_t> all;
  for (auto pair : node2server_) all.insert(all.begin(), pair.second.begin(), pair.second.end());
  return all;
}

const uint32_t SimpleIdMapper::kMaxNodeId;
const uint32_t SimpleIdMapper::kMaxThreadsPerNode;
const uint32_t SimpleIdMapper::kMaxBgThreadsPerNode;
const uint32_t SimpleIdMapper::kWorkerHelperThreadId;

}  // namespace csci5570
