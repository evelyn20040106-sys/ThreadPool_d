#include <iostream>
#include "ThreadPool.h"

int main() {
    std::mutex print_mutex;
    // 初始 2 个线程，最大 10 个，闲置 3 秒销毁
    DynamicThreadPool pool(2, 10, std::chrono::seconds(3));

    std::cout << "Starting tasks...\n";

    for (int i = 0; i < 15; ++i) {
        pool.enqueue([i, &print_mutex] {
            std::lock_guard<std::mutex> lock(print_mutex);
            std::cout << "Task " << i
                << " by Thread " << std::this_thread::get_id() << std::endl;
            });
    }

    // 第一波结束，观察线程数缩容
    std::this_thread::sleep_for(std::chrono::seconds(5));
    std::cout << "Pool should have shrunk now.\n";

    // 再次提交任务，观察扩容
    pool.enqueue([] { std::cout << "New task after shrink\n"; });

    std::this_thread::sleep_for(std::chrono::seconds(2));
    return 0;
}