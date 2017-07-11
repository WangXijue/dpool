#ifndef DPOOL_DPOOL_EXCEPTION_H_
#define DPOOL_DPOOL_EXCEPTION_H_

#include <exception>
#include <sstream>

namespace dpool {

class DPoolException: public std::exception {
  public:
    DPoolException(const std::string& errmsg, const char* file, int line) throw ()
     : errmsg_(errmsg), file_(file), line_(line) {
    }

    virtual ~DPoolException() throw () {}

    virtual const char* what() const throw () {
        return errmsg_.c_str();
    }

    std::string str() const throw () {
        std::ostringstream buff;
        buff << "dpool: " << errmsg_ << "/@" << file_ << ":" << line_;
        return buff.str();
    }

    const char* file() const throw () {
        return file_.c_str();
    }

    int line() const throw () {
        return line_;
    }

  private:
    const std::string errmsg_;
    const std::string file_;
    const int line_;
};

}

#endif // DPOOL_DPOOL_EXCEPTION_H_

