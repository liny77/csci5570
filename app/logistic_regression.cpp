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

using Sample = lib::LabeledSample<std::vector<int>, int>;
using DataStore = std::vector<Sample>;
using Parse = std::function<Sample(boost::string_ref, int)>;

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
  const auto kTableId = engine.CreateTable<double>(ModelType::SSP, StorageType::Map);  // table 0
  
  DLOG(INFO) << "create table";

  // 1.2 Load data
  engine.Barrier();
  DLOG(INFO) << "barrier";
  
  DataStore data;
  std::string url = "hdfs:///datasets/classification/a9a";
  int n_features = 123;
  Parse parse(lib::Parser<Sample, DataStore>::parse_libsvm);
  lib::AbstractDataLoader<Sample, DataStore>::load<Parse>(url, n_features, parse, &data);

  DLOG(INFO) << "data size: " << data.size();
  if (!data.empty()) {
    for (int i = 0; i < data[0].x_.size(); ++i) {
      if (data[0].x_[i] != 0) printf("%d:%d ", i, data[0].x_[i]);
    }
    printf("%d\n", data[0].y_);
  }

  // 3. Stop
  engine.StopEverything();
  return 0;
}
