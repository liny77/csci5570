#pragma once

#include <cinttypes>
#include <vector>

#include "base/abstract_partition_manager.hpp"
#include "base/magic.hpp"
#include "base/third_party/sarray.h"

#include "glog/logging.h"

#include "base/MurmurHash3.h"
#include <sstream>

namespace csci5570 {

class HashPartitionManager : public AbstractPartitionManager {
 public:
  HashPartitionManager(const std::vector<uint32_t>& server_thread_ids, int  virtual_node_cnt = 100)
                                    : AbstractPartitionManager(server_thread_ids), SEED_NUM(7) {
    std::stringstream ss;
    for (auto sid : server_thread_ids) {
        for (int i = 0; i < virtual_node_cnt; ++i) {
            uint32_t value;
            ss.str("SERVER_ID-");
            ss << sid << "VIRTUAL_NODE_ID-" << i;
            std::string key = ss.str();
            MurmurHash3_x86_32(static_cast<const void*>(key.c_str()), 
                                key.length(),
                                SEED_NUM,
                                static_cast<void*>(&value));
            hash2server_id[value] = sid;
        }
    }
  }

  void Slice(const Keys& keys, std::vector<std::pair<int, Keys>>* sliced) const override {
    std::map<int, Keys> resultMap;// server id, keys
    std::stringstream ss;
    for (auto key : keys) {
        ss.str("");
        ss << key;
        std::string temp = ss.str();
        uint32_t value;
        MurmurHash3_x86_32(static_cast<const void*>(temp.c_str()), 
                            temp.length(),
                            SEED_NUM,
                            static_cast<void*>(&value));
        
        auto it = hash2server_id.upper_bound(value);
        if (it == hash2server_id.end()) {
            resultMap[hash2server_id.begin()->second].push_back(key);
        } else {
            resultMap[it->second].push_back(key);
        }
    }
    for (auto it = resultMap.begin(); it != resultMap.end(); it++) {// collect slices
        sliced->push_back(std::make_pair(it->first, it->second));
    }
  }

  void Slice(const KVPairs& kvs, std::vector<std::pair<int, KVPairs>>* sliced) const override {
    std::map<int, KVPairs> resultMap;// server id, key[], value[]
    std::stringstream ss;
    auto &keys = kvs.first;
    auto &values = kvs.second;
    for (int i = 0; i < keys.size(); ++i) {
        ss.str("");
        ss << keys[i];
        std::string temp = ss.str();
        uint32_t value;
        MurmurHash3_x86_32(static_cast<const void*>(temp.c_str()), 
                            temp.length(),
                            SEED_NUM,
                            static_cast<void*>(&value));
        
        auto it = hash2server_id.upper_bound(value);
        if (it == hash2server_id.end()) {
            resultMap[hash2server_id.begin()->second].first.push_back(keys[i]);
            resultMap[hash2server_id.begin()->second].second.push_back(values[i]);
        } else {
            resultMap[it->second].first.push_back(keys[i]);
            resultMap[it->second].second.push_back(values[i]);
        }
    }
    for (auto it = resultMap.begin(); it != resultMap.end(); it++) {// collect slices
        sliced->push_back(std::make_pair(it->first, it->second));
    }
  }

 private:
    // hash value - server id
    std::map<uint32_t, uint32_t> hash2server_id;
    uint32_t SEED_NUM;
};

}  // namespace csci5570
