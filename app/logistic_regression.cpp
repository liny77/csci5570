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

using Record = std::vector<double>;
using Sample = lib::LabeledSample<Record, double>;
using DataStore = std::vector<Sample>;
using Parse = std::function<Sample(boost::string_ref, int)>;

DEFINE_string(config_file, "", "The config file path");
DEFINE_string(input, "", "The hdfs input url");

double lambda = 0.01;
double alpha = 0.0001;
int n_features = 2000;
std::string url = "hdfs:///datasets/classification/data3";

// multiplication
double mul(Record &pt, std::vector<double> &theta) {
	double sum = theta[0];// pt[0] == 1
	for (int i = 1; i < pt.size(); ++i) {
		sum += pt[i] * theta[i];
	}
	return sum;
}

// sigmoid function
double h(Record &pt, std::vector<double> &theta) {
	double sum = mul(pt, theta);
  LOG(INFO) << sum << " " << 1 / (1 + exp(-sum));
	return 1 / (1 + exp(-sum));
}

// 
double cost(Record &pt, std::vector<double> &theta, int pt_label) {
	double sum = h(pt, theta);
	return pt_label == 1 ? -log(sum) : -log(1 - sum);
}

double j(DataStore &samples, std::vector<int> &sample_index, std::vector<double> &theta) {
	double sum = 0;
	for (int i = 0; i < sample_index.size(); ++i) {
    auto idx = sample_index[i];
		sum += cost(samples[idx].x_, theta, samples[idx].y_);
	}
  sum /= sample_index.size();

  // regularization
	double temp = 0;
	for (int i = 1; i < theta.size(); ++i) {
		temp += theta[i] * theta[i];
	}
  temp /= 2 * sample_index.size();
	return sum + lambda * temp;
}

double update_theta_j(DataStore &samples, std::vector<int> &sample_index, std::vector<double> &theta, int index) {
	std::vector<double> diff;

	for (int i = 0; i < sample_index.size(); ++i) {
    auto idx = sample_index[i];
    int label = samples[idx].y_ == 1 ? 1 : 0;
		diff.push_back(h(samples[idx].x_, theta) - label);
	}
	
  double sum = 0;
  for (int i = 0; i < sample_index.size(); ++i) {
    auto idx = sample_index[i];
    sum += diff[i] * samples[idx].x_[index];
  }
  sum /= samples.size();
  if (index != 0) sum += lambda * theta[index] / samples.size();// regularization
  return theta[index] - alpha * sum;
}

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
  const auto kTableId = engine.CreateTable<double>(ModelType::BSP, StorageType::Map);  // table 0
  
  DLOG(INFO) << "create table";

  // 1.2 Load data
  engine.Barrier();
  DLOG(INFO) << "barrier";
  
  DataStore data;
  Parse parse(lib::Parser<Sample, DataStore>::parse_libsvm);
  lib::AbstractDataLoader<Sample, DataStore>::load<Parse>(url, n_features, parse, &data);

  LOG(INFO) << "first line";
  for (int i = 0; i <= n_features; ++i) {
    if (data[0].x_[i] != 0) printf("%d:%lf ",i,  data[0].x_[i]);
  }
  printf("%d\n", data[0].y_);


  DLOG(INFO) << "data size: " << data.size();
  
  // 2. Start training task
  MLTask task;
  task.SetWorkerAlloc({{0, 1}});  // 3 workers on node 0
  task.SetTables({kTableId});     // Use table 0
  task.SetLambda([kTableId, &data](const Info& info) {
    LOG(INFO) << info.DebugString();

    KVClientTable<double> table = info.CreateKVClientTable<double>(kTableId);

    // BSP
    int round = 100;
    int p_start = 0;
    int p_end = n_features;
    int batch_size = data.size() * 0.01 + 1;
    LOG(INFO) << "batch size " << batch_size;

    std::vector<Key> all_keys;// parameters index
    for (int i = 0; i <= n_features; ++i) all_keys.push_back((Key)i);
    std::vector<double> all_parameters;//  real parameters

    std::vector<Key> target_keys;// parameters for this worker to update
    for (int i = p_start; i <= p_end; ++i) target_keys.push_back((Key)i);
    std::vector<double> target_vals(target_keys.size(), 1);// parameters for this worker to update, initial values are all 1

    std::vector<int> data_index(batch_size);//  random picked record's index

    all_parameters.clear();
    table.Get(all_keys, &all_parameters);
    table.Add(target_keys, target_vals);// initial parameters
    table.Clock();

    for (int i = 0; i < round; ++i) {
        all_parameters.clear();
        table.Get(all_keys, &all_parameters);// get old parameters

        for (int j = 0; j < data_index.size(); ++j) {// randomly pick data
          data_index[j] = static_cast<int>((rand() * 1.0 / RAND_MAX) * (data.size() - 1));// random record
        }

        double current_cost = j(data, data_index, all_parameters);
        LOG(INFO) << "before " << i << " round Cost: " << current_cost;

        for (int j = 0; j < target_vals.size(); ++j) {
            target_vals[j] = update_theta_j(data, data_index, all_parameters, p_start + j);
        }
        table.Add(target_keys, target_vals);// update parameters
        table.Clock();
    }

    all_parameters.clear();
    table.Get(all_keys, &all_parameters);
    for (int i = 0; i < all_parameters.size(); ++i) {
      printf("%lf ", all_parameters[i]);
    }
    printf("\n");

  });

  engine.Run(task);

  // 3. Stop
  engine.StopEverything();
  return 0;
}
