#pragma once

#include <functional>
#include <string>

#include "boost/utility/string_ref.hpp"

#include "lib/parser.hpp"
#include "io/line_input_format.hpp"

namespace csci5570 {
namespace lib {

// template <typename Sample, template <typename> typename DataStore<Sample>>
template <typename Sample, typename DataStore>
class AbstractDataLoader {
 public:
  /**
   * Load samples from the url into datastore
   *
   * @param url          input file/directory
   * @param n_features   the number of features in the dataset
   * @param parse        a parsing function
   * @param datastore    a container for the samples / external in-memory storage abstraction
   */
  template <typename Parse>  // e.g. std::function<Sample(boost::string_ref, int)>
  static void load(std::string url, int n_features, Parse parse, DataStore* datastore) {
    // 1. Connect to the data source, e.g. HDFS, via the modules in io
    // 2. Extract and parse lines
    // 3. Put samples into datastore
    // LineInputFormat infmt(url, num_threads, id, coordinator, hostname, hdfs_namenode, hdfs_namenode_port);
    // LOG(INFO) << "Line input is well prepared";
    // bool success = true;
    // int count = 0;
    // boost::string_ref record;
    // while (true) {
    //   success = infmt.next(record);
    //   if (success == false) break;
    //   datastore->push_back(parse(record, n_features));
    //   ++count;
    // }
    // LOG(INFO) << "The number of lines in " << url << " is " << count;
  }
};  // class AbstractDataLoader

}  // namespace lib
}  // namespace csci5570
