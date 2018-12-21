#include <cmath>

#include <iostream>
#include <random>
#include <thread>

#include "gflags/gflags.h"
#include "glog/logging.h"

#include "driver/engine.hpp"
#include "lib/abstract_data_loader.hpp"
#include "lib/labeled_sample.hpp"
#include "lib/parser.hpp"

using namespace csci5570;

using Sample = double;
using DataStore = std::vector<Sample>;

DEFINE_string(config_file, "", "The config file path");
DEFINE_string(input, "", "The hdfs input url");

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  FLAGS_stderrthreshold = 0;
  FLAGS_colorlogtostderr = true;

  LOG(INFO) << FLAGS_config_file;

  Node node{0, "localhost", 12353};
  Engine engine(node, {node});

  // 1. Start system
  engine.StartEverything();

  // 1.1 Create table
  // add range
  const auto kTableId = engine.CreateTable<double>(ModelType::ASP, StorageType::Map);  // table 0
  
  DLOG(INFO) << "create table";

  // 1.2 Load data
  engine.Barrier();

  DLOG(INFO) << "barrier";

  // 2. Start training task
  MLTask task;
  task.SetWorkerAlloc({{0, 1}});  // 3 workers on node 0
  task.SetTables({kTableId});     // Use table 0
  task.SetLambda([kTableId](const Info& info) {
    LOG(INFO) << info.DebugString();

    KVClientTable<double> table = info.CreateKVClientTable<double>(kTableId);

    for (int i = 0; i < 10; ++i) {
      if (i % 1000 == 0) DLOG(INFO) << "worker " << info.thread_id << " i: " << i;
      std::vector<Key> keys{info.worker_id};

      std::vector<double> ret;
      table.Get(keys, &ret);
      LOG(INFO) << i << " round " << info.worker_id << " " << ret[0];

      std::vector<double> vals{i * 1.0 + info.worker_id};
      table.Add(keys, vals);
      table.Clock();
    }
  });

  engine.Run(task);

  // 3. Stop
  engine.StopEverything();
  return 0;
}
