#include "server/server_thread.hpp"

#include "glog/logging.h"

namespace csci5570 {

void ServerThread::RegisterModel(uint32_t model_id, std::unique_ptr<AbstractModel>&& model) {
    // insert if key is not existed, otherwise do nothing
    models_.insert(std::make_pair(model_id, std::move(model)));
}

AbstractModel* ServerThread::GetModel(uint32_t model_id) {
    // return model if existed, otherwise return nullptr
    auto it = models_.find(model_id);
    return (it != models_.end()) ? it->second.get() : nullptr;
}

void ServerThread::Main() {
    Message msg;
    while (true) {
        work_queue_.WaitAndPop(&msg);
        if (msg.meta.flag == Flag::kExit) break;
        auto *model = GetModel(msg.meta.model_id);
        switch (msg.meta.flag) {
            case Flag::kClock:
                model->Clock(msg);
                break;
            case Flag::kAdd:
                model->Add(msg);
                break;
            case Flag::kGet:
                model->Get(msg);
                break;
        }
    }
}

}  // namespace csci5570

