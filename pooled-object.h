#ifndef DPOOL_POOLED_OBJECT_H_
#define DPOOL_POOLED_OBJECT_H_

#include <mutex>          // std::mutex
#include <memory>         // std::shared_ptr

namespace dpool {

struct InetSocketAddress {
    InetSocketAddress(const char* host, uint16_t port) : host(host), port(port) {}
    InetSocketAddress(const std::string& host, uint16_t port) : host(host), port(port) {}

    const std::string to_string() const {
        return host + ":" + std::to_string(port);
    }

    const std::string host;
    const uint16_t port;
};

// Poolable represents a connection to a server.
class PooledObject {
  public:
    PooledObject(const InetSocketAddress& addr, const int connTimeout, const int dataTimeout)
      : serverAddr_(addr), connTimeout_(connTimeout), dataTimeout_(dataTimeout) {
    }

    virtual ~PooledObject() {}

    void lock() {
        mtx_.lock();
    }

    void unlock() {
        mtx_.unlock();
    }

    void* getDataSource() {
        return dataSource_;
    }

    void setDataSource(void* shard) {
        dataSource_ = shard;
    }

    bool isBorrowed() {
        return borrowed_;
    }

    void setBorrowed(bool v) {
        borrowed_ = v;
    }

    virtual void open() throw (DPoolException) = 0;

    const InetSocketAddress& getServerAddr() const {
        return serverAddr_;
    }

  private:
    void* dataSource_;
    bool borrowed_;
    std::mutex mtx_;

  protected:
    const dpool::InetSocketAddress serverAddr_;
    const int connTimeout_;
    const int dataTimeout_;
};

struct PoolConfig {
    PoolConfig() : connTimeoutMs(100), dataTimeoutMs(100), maxIdle(10), maxActive(100), maxFails(5) {}

    PoolConfig(int connTimeoutMs, int dataTimeoutMs, int maxIdle, int maxActive = 100, int maxFails = 5)
        : connTimeoutMs(100), dataTimeoutMs(100), maxIdle(maxIdle),
          maxActive(maxActive), maxFails(maxFails) {
    }
    const int maxIdle;
    const int maxActive;
    const int maxFails;
    const int connTimeoutMs;
    const int dataTimeoutMs;
};

struct PoolStats {
    PoolStats(const InetSocketAddress& addr)
        : server(addr), available(true),
          numActive(0), numGet(0),
          numPut(0), numBroken(0),
          numDial(0), numDialFail(0),
          numEvict(0), numClose(0) {
    }

    void reset() {
        available = true;
        numActive = 0;
        numGet = 0;
        numPut = 0;
        numBroken = 0;
        numDial = 0;
        numDialFail = 0;
        numEvict = 0;
        numClose = 0;
    }

    const InetSocketAddress server;
    bool available;
    int  numActive;
    long numGet;
    long numPut;
    long numBroken;
    long numDial;
    long numDialFail;
    long numEvict;
    long numClose;
};

} // namespace dpool

#endif // DPOOL_POOLED_OBJECT_H_
