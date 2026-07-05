#pragma once
#include <vector>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <future>
#include <atomic>
#include <type_traits>


class DynamicThreadPool
{

private:

	struct PoolData
	{
		std::mutex mtx;	//互斥锁
		std::condition_variable cv;	//条件变量
		std::queue<std::function<void()>> tasks;	//任务队列
		std::atomic<bool> shutdown{ false };	//关闭标志
		std::atomic<size_t> cur_threads{ 0 };	//当前线程数
		std::atomic<size_t> idle_threads{ 0 };	//空闲线程数
	};

	std::shared_ptr<PoolData> data;	//共享数据指针
	size_t min_threads;	//最小线程数
	size_t max_threads;	//最大线程数
	std::chrono::milliseconds idle_timeout;	//空闲超时时间

public:

	explicit DynamicThreadPool(size_t min = 4, size_t max = 20, std::chrono::milliseconds timeout = std::chrono::seconds(2))
		:data(std::make_shared<PoolData>()), min_threads(min), max_threads(max), idle_timeout(timeout)
	{
		for (size_t i = 0; i < min_threads; i++)
		{
			add_worker(true);	//初始化核心线程
		}
	}

	~DynamicThreadPool()
	{
		if (data)
		{
			data->shutdown = true;	//更新关闭标志
			data->cv.notify_all();	//唤醒所有线程
		}
	}

	template<typename F, typename... Args>
	auto enqueue(F&& f, Args&&... args)	//可以接收任意数量和类型的参数
		-> std::future <std::invoke_result_t<F, Args...>>
	{
		using ReturnType = std::invoke_result_t<F, Args...>;	//记住返回类型

		auto task = std::make_shared<std::packaged_task<ReturnType()>>	//制作package_task，包装一个无参函数
			(
				std::bind(std::forward<F>(f), std::forward<Args>(args)...)	//将函数和参数绑定，做成一个无参的可执行函数
			);

		std::future<ReturnType> res = task->get_future();	//获取到此函数的future
		{
			std::lock_guard<std::mutex> lock(data->mtx);	//上锁
			if (data->shutdown)	throw std::runtime_error("threadpool is shutting down");	//如果线程池关闭就抛出异常
			data->tasks.emplace	//将任务加入任务队列
			(
				[task = std::move(task)]()
				{
					(*task)();
				}
			);
		}
		data->cv.notify_one();	//唤醒一个线程

		if (data->idle_threads == 0 && data->cur_threads < max_threads)	//如果没有空闲线程，且不超过最大线程数，则创建新线程
		{
			add_worker(false);
		}

		return res;
	}

private:

	void add_worker(bool is_core)
	{
		data->cur_threads++;
		std::thread([data = this->data, is_core, timeout = this->idle_timeout, min_count = this->min_threads]() {
			while (true)
			{
				std::function<void()> task;
				{
					std::unique_lock<std::mutex> lock(data->mtx);	//上锁
					data->idle_threads++;	//空闲线程+1
					auto wait_condition = [&] { return data->shutdown || !data->tasks.empty(); };	//检测是否关闭或者任务不为空

					if (is_core)	//判断是否为核心线程
					{
						data->cv.wait(lock, wait_condition);	//核心线程永远等待且不超时
					}
					else
					{
						if (!data->cv.wait_for(lock, timeout, wait_condition))	//一定时间下，检查是否关闭或任务不为空
						{
							if (data->cur_threads > min_count) //如果当前线程大于最小线程数
							{
								data->cur_threads--;
								data->idle_threads--;
								return;	//线程退出
							}
						}
					}
					if (data->shutdown && data->tasks.empty())	//如果处于关闭状态且任务空了
					{
						data->cur_threads--;
						data->idle_threads--;
						return;	//退出
					}

					data->idle_threads--;	//空闲线程-1
					task = std::move(data->tasks.front());	//取出任务
					data->tasks.pop();
				}
				task();	//干活
			}

		}).detach();
	}
};