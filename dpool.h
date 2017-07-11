#ifndef DPOOL_DPOOL_H_
#define DPOOL_DPOOL_H_

#include <iostream>
#include <cassert>
#include <cstdint>
#include <vector>
#include <functional>
#include <list>
#include <atomic>
#include <thread>
#include <mutex>
#include <condition_variable>

#include "dpool-exception.h"
#include "pooled-object.h"
#include "pool-shard.h"

namespace dpool {

template <typename T>
class DPool {
  public:
    DPool(const std::vector<InetSocketAddress>& servers, PoolConfig config)
        : poolConfig_(config), closed_(false) {
        assert(!servers.empty());
        numAvailable_ = servers.size();
        for (auto it = servers.begin(); it != servers.end(); it++) {
            servers_.push_back(*it);
            PoolShard<T>* shard = new PoolShard<T>(*it, poolConfig_);
            poolShards_.push_back(shard);
        }

        healthCheckThread_ = std::thread(&DPool<T>::healthCheck, this);
    }

    virtual ~DPool() {
        if (!closed_.load(std::memory_order_relaxed)) {
            shutdown();
        }
        for (int i = 0; i < poolShards_.size(); ++i) {
            delete poolShards_[i];
        }
    }

    DPool(const DPool&) = delete;
    DPool& operator=(const DPool&) = delete;    // noncopyable

    std::shared_ptr<T> get() throw (DPoolException) {
        unsigned localIndex = index_.fetch_add(1);

        for (unsigned tries=0; tries < 5; ++tries) {
            int idx = ((localIndex + tries) % servers_.size());

            if (!poolShards_[idx]->isAvailable()) {
                index_.fetch_add(1);
                continue;
            }

            std::shared_ptr<T> pc = poolShards_[idx]->get();
            if (pc == nullptr) {
                index_.fetch_add(1);
                continue;
            }
            return pc;
        }

        throw DPoolException("failed to get connection after max retries", __FILE__, __LINE__);
    }

    void put(std::shared_ptr<T> pc, bool broken = false) {
        assert(pc != nullptr && "cannot return nullptr");
        PoolShard<T>* shard = (PoolShard<T>*)(pc->getDataSource());
        assert(shard != nullptr && "shard should not be null");
        return shard->put(pc, broken);
    }

    void shutdown() {
        bool expected = false;
        if (!(closed_.compare_exchange_strong(expected, true))) {
            std::cerr << "dpool: pool already closed" << std::endl;
            return;
        }
        healthCheckThread_.join();
        // TODO
    }

    // Pool statistics for monitor
    void getPoolStats(std::vector<PoolStats>& statsList) {
        statsList.clear();
        for (auto it = poolShards_.begin(); it != poolShards_.end(); it++) {
            PoolStats st((*it)->getServerAddr());
            (*it)->getShardStats(st);
            statsList.push_back(st);
        }
    }

  private:
    void markAvailable(PoolShard<T>* shard, bool b) {
        if (b) {
            if (shard->markAvailable(true)) {
                numAvailable_++;
                std::cerr << "dpool: server recovered - " << shard->getServerAddr().to_string() << std::endl;
            }
        } else {
            // Ensure that at most 1/3 servers can be marked as unavaialable
            if (numAvailable_*3 > servers_.size()*2) {
                if (shard->markAvailable(false)) {
                    numAvailable_--;
                    std::cerr << "dpool: mark server unvailable: " << shard->getServerAddr().to_string() << std::endl;
                }
            } else {
                std::cerr << "dpool: server cannot be marked as unavailable due to too many failed shards, "
                << "numAvailable: " << numAvailable_ << ", totalShards: " << servers_.size() << std::endl;
                //shard.server, dp.numAvailable, totalServers)
            }
        }
    }

    // Check if the server specified by @addr is OK.
    bool checkServer(const InetSocketAddress& addr) {
        for (int tries=0; tries < 2; tries++) {
            int connectTimeout = 100, readWriteTimeout = 100;
            std::shared_ptr<T> c = std::make_shared<T>(addr, connectTimeout, readWriteTimeout);
            try {
                c->open();
            } catch (DPoolException& ex) {
                std::cerr << "Connect server failed: " << addr.to_string() << std::endl;
                continue;
            }
            return true;
        }
        return false;
    }

    // Health checker thread routine 
    void healthCheck() {
        while (!closed_.load(std::memory_order_relaxed)) {
            std::this_thread::sleep_for(std::chrono::seconds(1));

            for (auto it = poolShards_.begin(); it != poolShards_.end(); it++) {
                auto shard = *it;
                if (!shard->isSuspectable() && shard->isAvailable()) {
                    continue;
                }

                bool ok = checkServer(shard->getServerAddr());
                markAvailable(shard, ok);
            }
        }
        std::cout << "stop health check thread, closed: " << closed_.load() << std::endl;
    }

  private:
    // Server address list, e.t. {"127.0.0.1:8080", "127.0.0.1:8081"}
    std::vector<InetSocketAddress> servers_;

    // Sharded pool by server address
    std::vector<PoolShard<T>* > poolShards_;

    // Pool configuration, e.t. maxIdle, maxActive, ...
    const PoolConfig poolConfig_;

    // @atomic index to pick the next shard
    std::atomic<unsigned> index_;

    // Current available servers
    int numAvailable_;

    int maxRetry_;

    // Health check thread
    std::thread healthCheckThread_;

    std::atomic<bool> closed_;
};

} // namespace dpool

#endif // DPOOL_DPOOL_H_
