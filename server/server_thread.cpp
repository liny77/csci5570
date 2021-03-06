#include "server/server_thread.hpp"

#include "glog/logging.h"

namespace csci5570 {

void ServerThread::RegisterModel(uint32_t model_id, std::unique_ptr<AbstractModel>&& model) {
    // insert model, if existed update model
    models_[model_id] = std::move(model);
}

AbstractModel* ServerThread::GetModel(uint32_t model_id) {
    // return model if existed, otherwise return nullptr
    auto it = models_.find(model_id);
    return (it != models_.end()) ? it->second.get() : nullptr;
}

void ServerThread::Main() {
    DLOG(INFO) << "Server " << id_ << " is running";
    Message msg;
    while (true) {
        GetWorkQueue()->WaitAndPop(&msg);
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
            case Flag::kResetWorkerInModel:
                model->ResetWorker(msg);
                break;
        }
    }
}

}  // namespace csci5570

