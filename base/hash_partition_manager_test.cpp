#include "glog/logging.h"
#include "gtest/gtest.h"

#include "base/magic.hpp"
#include "base/hash_partition_manager.h"

namespace csci5570 {

class TestHashPartitionManager : public testing::Test {
 protected:
  void SetUp() {}
  void TearDown() {}
};  // class TestHashPartitionManager

TEST_F(TestHashPartitionManager, Init) { HashPartitionManager pm({0, 1, 2}); }

TEST_F(TestHashPartitionManager, SliceKeys) {
  HashPartitionManager pm({0, 1, 2});
  third_party::SArray<Key> keys({2, 10000, 9});
  std::vector<std::pair<int, AbstractPartitionManager::Keys>> sliced;
  pm.Slice(keys, &sliced);

  for (auto slice : sliced) {
    printf("server id: %d, key ", slice.first);
    for (auto id : slice.second) {
        printf(" %d", id);
    }
    printf("\n");
  }
}

TEST_F(TestHashPartitionManager, SliceKVs) {
  HashPartitionManager pm({0, 1, 2});
  third_party::SArray<Key> keys({2, 10000, 9});
  third_party::SArray<double> vals({.2, .5, .9});
  std::vector<std::pair<int, AbstractPartitionManager::KVPairs>> sliced;
  pm.Slice(std::make_pair(keys, vals), &sliced);

  for (auto slice : sliced) {
    printf("server id: %d", slice.first);
    for (int i = 0; i < slice.second.first.size(); ++i)
        printf(" (%d, %lf)", slice.second.first[i], slice.second.second[i]);
    printf("\n");
  }
}

}  // namespace csci5570
