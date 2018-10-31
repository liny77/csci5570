#include "server/util/pending_buffer.hpp"

namespace csci5570 {

// if clock not exist, return an empty vector
std::vector<Message> PendingBuffer::Pop(const int clock) {
  auto it = buffer_map.find(clock);
  auto rslt = it != buffer_map.end() ? it->second : std::vector<Message>();
  buffer_map.erase(it);
  return rslt;
}

void PendingBuffer::Push(const int clock, Message& msg) {
  buffer_map[clock].push_back(msg);
}

int PendingBuffer::Size(const int progress) {
  auto it = buffer_map.find(progress);
  return it != buffer_map.end() ? it->second.size() : 0;
}

}  // namespace csci5570
