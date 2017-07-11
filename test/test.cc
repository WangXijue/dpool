#include <iostream>

#include "hiredis/hiredis.h"
#include <unistd.h>
#include <sys/time.h>

#include "dpool.h"

class PooledRedisContext : public dpool::PooledObject {
  public:
    typedef std::shared_ptr<PooledRedisContext> SharedPtr;

    PooledRedisContext(const dpool::InetSocketAddress& addr, const int connTimeout, const int dataTimeout)
     : ctx(nullptr), dpool::PooledObject(addr, connTimeout, dataTimeout) {
    }

    virtual ~PooledRedisContext() {
        if (ctx != nullptr) {
            std::cout << "Free redis context: " << serverAddr_.to_string() << std::endl;
            redisFree(ctx);
            ctx = nullptr;
        }
    }

    virtual void open() throw (dpool::DPoolException) override {
        struct timeval tv;
        tv.tv_sec = 0; tv.tv_usec = 1000 * this->connTimeout_;
        ctx = redisConnectWithTimeout(serverAddr_.host.c_str(), serverAddr_.port, tv);
        if (ctx == nullptr) {
            throw dpool::DPoolException("can't allocate redis context", __FILE__, __LINE__);
        }
        if (ctx->err) {
            std::string errmsg("Failed to connect redis: ");
            errmsg += ctx->errstr;
            throw dpool::DPoolException(errmsg, __FILE__, __LINE__);
        }

        tv.tv_usec = 1000 * this->dataTimeout_;
        redisSetTimeout(ctx, tv);
        return;
    }

  public: // FIXME
    redisContext *ctx;
};

int main() {
    const dpool::PoolConfig config;

    // Server address list
    dpool::InetSocketAddress server1("127.0.0.1", 6379);
    dpool::InetSocketAddress server2("127.0.0.1", 6380);
    dpool::InetSocketAddress server3("127.0.0.1", 6381);

    std::vector<dpool::InetSocketAddress> serverList;
    serverList.push_back(server1);
    serverList.push_back(server2);
    serverList.push_back(server3);

    dpool::DPool<PooledRedisContext> dp(serverList, config);

    for (int i = 0; i < 10; i++) {
        PooledRedisContext::SharedPtr c;
        try {
            c = dp.get();
        } catch (dpool::DPoolException& ex) {
            std::cout << ex.what() << std::endl;
            return EXIT_FAILURE;
        }
    
        std::cout << "Get pooled connection: " << (c->getServerAddr()).to_string() << std::endl;
        redisReply* reply = (redisReply*) redisCommand(c->ctx, "SET foo bar");
        if (reply != nullptr) {
            std::cout << "reply: " << reply->str << std::endl; // reply for SET
        }
        dp.put(c, true);
    }

    sleep(1000);
    return 0;
}
