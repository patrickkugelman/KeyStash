#include "persist/worker.h"

#include <utility>

namespace persist {

Worker::Worker() : thread_([this] { run(); }) {}

Worker::~Worker() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    stopping_ = true;
  }
  wake_.notify_one();
  thread_.join();
}

void Worker::submit(std::function<void()> task) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    queue_.push_back(std::move(task));
  }
  wake_.notify_one();
}

void Worker::run() {
  std::unique_lock<std::mutex> lock(mutex_);
  while (true) {
    wake_.wait(lock, [this] { return stopping_ || !queue_.empty(); });
    if (queue_.empty()) return;  // stopping and drained
    std::function<void()> task = std::move(queue_.front());
    queue_.pop_front();
    lock.unlock();
    task();
    lock.lock();
  }
}

}  // namespace persist
