#ifndef DPOOL_POOL_SHARD_H_
#define DPOOL_POOL_SHARD_H_

#include "pooled-object.h"

namespace dpool {

template <typename T>
class PoolShard {
  public:
    PoolShard(const InetSocketAddress server, const PoolConfig& config)
        : server_(server), available_(true),
         fails_(0), kMaxWait_(3), kMaxIdle_(config.maxIdle), stats_(server),
         kMaxActive_(config.maxActive), kMaxFails_(config.maxFails), active_(0),
         closed_(false), connTimeoutMs_(config.connTimeoutMs), dataTimeoutMs_(config.dataTimeoutMs)  {
    }

    PoolShard(const PoolShard&) = delete;
    PoolShard& operator=(const PoolShard&) = delete;    // noncopyable

    virtual ~PoolShard() {
        close();
    }

    void close() {
        bool expected = false;
        if (!(closed_.compare_exchange_strong(expected, true))) {
            std::cerr << "dpool: shard already closed" << std::endl;
            return;
        }
        empty();
    }

    std::shared_ptr<T> get() {
        auto start = std::chrono::system_clock::now();
        std::shared_ptr<T> c;

        std::unique_lock<std::mutex> lck(mtx_);

        stats_.numGet++;

        while (true) {
            if (!idle_.empty()) {
                c = idle_.front();
                idle_.pop_front();
                c->setBorrowed(true);
                lck.unlock();
                return c;
            }

            if (closed_.load(std::memory_order_relaxed)) {
                lck.unlock();
                std::cerr << "dpool: get on closed pool shard " << server_.to_string() << std::endl;
                return nullptr;
            }

            if (kMaxActive_ == 0 || active_ < kMaxActive_) {
                active_++;
                stats_.numDial++;
                lck.unlock();

                c = std::make_shared<T>(server_, connTimeoutMs_, dataTimeoutMs_);
                try {
                    c->open();
                    fails_.store(0, std::memory_order_relaxed);
                    c->setDataSource(this);
                    c->setBorrowed(true);
                    return c;
                } catch (DPoolException& ex) {
                    fails_.fetch_add(1, std::memory_order_relaxed);
                    lck.lock();
                    active_--;
                    stats_.numDialFail++;
                    lck.unlock();
                    cv_.notify_one();
                    std::cerr << "dpool: failed to create connection on pool shard "
                            << ex.what() << std::endl;
                    return nullptr;
                }
            }

            std::cerr << "dpool: failed to dial connection to server: " << (server_.to_string()) 
                      << " , active: " << active_ << std::endl;

            if (!kWait_) {
                lck.unlock();
                return nullptr;
            }

            auto abs_time = start + std::chrono::milliseconds(kMaxWait_);
            if (cv_.wait_until(lck, abs_time) == std::cv_status::timeout) {
                lck.unlock();
                std::cerr << "dpool: timedout to wait idle connection on pool shard "
                        << (server_.to_string()) << std::endl;
                return nullptr;
            }
        }
    }

    void put(std::shared_ptr<T> pc, bool broken) {
        std::unique_lock<std::mutex> lck(mtx_);

        stats_.numPut++;

        if (!pc->isBorrowed()) {
            lck.unlock();
            return;
        } 
        pc->setBorrowed(false);

        if (broken) {
            fails_.fetch_add(1, std::memory_order_relaxed);
            stats_.numBroken++;
        } else {
            fails_.store(0, std::memory_order_relaxed);
        }

        if (!closed_.load(std::memory_order_relaxed) && !broken) {
            idle_.push_front(pc);
            if (idle_.size() > kMaxIdle_) {
                pc = idle_.back();
                idle_.pop_back();
				stats_.numEvict++;
            } else {
                pc = nullptr;
            }
        }

        if (pc == nullptr) {
            lck.unlock();
            cv_.notify_one();
            return;
        }

        active_--;
        stats_.numClose++;
        lck.unlock();
        cv_.notify_one();
        //connFactory_.close(pc);
        return;
    }

    bool isAvailable() {
        return available_.load(std::memory_order_relaxed);
    }

    bool isSuspectable() {
        return (fails_.load(std::memory_order_relaxed) >= kMaxFails_); 
    }

    // @return - true if the underlying atomic value was changed, false otherwise.
    bool markAvailable(const bool avail) {
        bool expected = !avail;
        return available_.compare_exchange_strong(expected, avail);
    }

    const InetSocketAddress& getServerAddr() const {
        return server_;
    }

    void getShardStats(PoolStats& st) {
        st.available = available_.load(std::memory_order_relaxed);

        std::lock_guard<std::mutex> lck(mtx_);
        st.numActive = active_;
        st.numGet = stats_.numGet;
        st.numPut = stats_.numPut;
        st.numDial = stats_.numDial;
        st.numDialFail = stats_.numDialFail;
        st.numBroken = stats_.numBroken;
        st.numEvict = stats_.numEvict;
        st.numClose = stats_.numClose;
        stats_.reset();
    }

  private:
    void empty() {
        std::unique_lock<std::mutex> lck(mtx_);

        while (!idle_.empty()) {
            std::shared_ptr<T> c = idle_.front();
            idle_.pop_front();
            active_--;
            stats_.numClose++;
            lck.unlock();
            cv_.notify_one();
            lck.lock();
            //lck.unlock();
            //connFactory_.close(c);
            //lck.lock();
        }
    }

  private:
    // Maximum number of idle connections in the pool.
    const int kMaxIdle_;

    // Maximum number of connections allocated by the pool at a given time.
    // When zero, there is no limit on the number of connections in the pool.
    const int32_t kMaxActive_;

    // Current number of active connections
    int32_t active_;

    // Close connections after remaining idle for this duration. If the value
    // is zero, then idle connections are not closed. Applications should set
    // the timeout to a value less than the server's timeout.
    // XXX: current not used
    const int kIdleTimeout_ = 500;

    // If wait is true and the pool is at the maxIdle limit, then Get() waits
    // for a connection to be returned to the pool before returning.
    // XXX: current not used
    const bool kWait_ = false;

    // The maximum number of milliseconds that the pool will wait (when there
    // are no available connections and the maxActive has been reached) for a
    // connection to be returned before returning an error. Default value is 3
    // (3 milliseconds)
    const int kMaxWait_;

    // @atomic
    std::atomic<bool> closed_;

    // Stack of idle Poolable with most recently used at the front.
    std::list<std::shared_ptr<T>> idle_;

    // Server address, e.g. "127.0.0.1:8080"
    const InetSocketAddress server_;

    // If marked as unavailable, then the checking goroutine will check it availability periodically.
    // A server is "available" if we can connnect to it, and respond to Ping() request of client.
    // Since no atomic boolean provided in Golang, we use uint32 instead.
    std::atomic<bool> available_;

    // The failure count in succession. If the fails reached the threshold of "unavailable",
    // then this server should be marked as "unavailable", and we will not get connection
    // from it until recovered.
    // The idea of "fails" & "maxFails" is borrowed from Nginx.
    // @atomic
    std::atomic<unsigned> fails_;

    // The idea of "fails" & "maxFails" is borrowed from Nginx.
    const uint32_t kMaxFails_;

    const int connTimeoutMs_;

    const int dataTimeoutMs_;

    // Mutex to protect idle connections & active
    std::mutex mtx_;

    std::condition_variable cv_;

    // stats PoolStats
    PoolStats stats_;
};

} // namespace dpool

#endif // DPOOL_POOL_SHARD_H_
