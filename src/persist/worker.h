#pragma once

#include <condition_variable>
#include <deque>
#include <functional>
#include <mutex>
#include <thread>

namespace persist {

// one thread, runs tasks in order, the destructor drains the queue
class Worker {
 public:
  Worker();
  ~Worker();
  Worker(const Worker&) = delete;
  Worker& operator=(const Worker&) = delete;

  void submit(std::function<void()> task);

 private:
  void run();

  std::mutex mutex_;
  std::condition_variable wake_;
  std::deque<std::function<void()>> queue_;
  bool stopping_ = false;
  std::thread thread_;  // keep last, it starts in the constructor
};

}  // namespace persist
