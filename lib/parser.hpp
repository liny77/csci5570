#pragma once

#include "boost/utility/string_ref.hpp"

namespace csci5570 {
namespace lib {

template <typename Sample, typename DataStore>
class Parser {
 public:
  /**
   * Parsing logic for one line in file
   *
   * @param line    a line read from the input file
   */
  // format
  // -1 4:1 6:1 16:1 26:1 35:1 45:1 49:1 64:1 71:1 72:1 74:1 76:1 78:1 101:1 
  // +1 3:1 6:1 18:1 20:1 37:1 40:1 51:1 63:1 71:1 73:1 74:1 76:1 82:1 83:1 
  static Sample parse_libsvm(boost::string_ref line, int n_features) {
    // check the LibSVM format and complete the parsing
    // hints: you may use boost::tokenizer, std::strtok_r, std::stringstream or any method you like
    // so far we tried all the tree and found std::strtok_r is fastest :)
    Sample sample;
    sample.x_.resize(n_features + 1, 0);
    std::stringstream ss;
    ss << std::string(line.begin(), line.end());
    int index, value;
    char colon;
    ss >> sample.y_;
    while (ss >> index >> colon >> value) {
      sample.x_[index] = value;
    }
    return sample;
  }

  static Sample parse_mnist(boost::string_ref line, int n_features) {
    // check the MNIST format and complete the parsing
  }

  // You may implement other parsing logic

};  // class Parser

}  // namespace lib
}  // namespace csci5570
