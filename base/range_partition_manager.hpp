#pragma once

#include <cinttypes>
#include <vector>

#include "base/abstract_partition_manager.hpp"
#include "base/magic.hpp"
#include "base/third_party/sarray.h"

#include "glog/logging.h"

namespace csci5570 {

class RangePartitionManager : public AbstractPartitionManager {
 public:
  RangePartitionManager(const std::vector<uint32_t>& server_thread_ids, const std::vector<third_party::Range>& ranges)
      : AbstractPartitionManager(server_thread_ids), ranges_(ranges.begin(), ranges.end()) {}

  void Slice(const Keys& keys, std::vector<std::pair<int, Keys>>* sliced) const override {
      std::map<int, Keys> resultMap;// server id, keys
      for (auto key : keys) {
          for (int i = 0; i < ranges_.size(); ++i) {
              if (ranges_[i].begin() <= key && key < ranges_[i].end()) {//  begin <= key < end
                  resultMap[server_thread_ids_[i]].push_back(key);
                  break;
              }
          }
      }
      for (auto it = resultMap.begin(); it != resultMap.end(); it++) {// collect slices
          sliced->push_back(std::make_pair(it->first, it->second));
      }
  }

  void Slice(const KVPairs& kvs, std::vector<std::pair<int, KVPairs>>* sliced) const override {
      std::map<int, KVPairs> resultMap;// server id, keys
      for (int k = 0; k < kvs.first.size(); ++k) {
          Key key = kvs.first[k];// get current key
          double value = kvs.second[k];// get current value
          for (int i = 0; i < ranges_.size(); ++i) {
              if (ranges_[i].begin() <= key && key < ranges_[i].end()) {//  begin <= key < end
                  resultMap[server_thread_ids_[i]].first.push_back(key);
                  resultMap[server_thread_ids_[i]].second.push_back(value);
                  break;
              }
          }
      }
      for (auto it = resultMap.begin(); it != resultMap.end(); it++) {// collect slices
          sliced->push_back(std::make_pair(it->first, it->second));
      }
  }

 private:
  std::vector<third_party::Range> ranges_;
};

}  // namespace csci5570
