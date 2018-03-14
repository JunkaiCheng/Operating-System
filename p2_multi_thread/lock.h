//
//  lock.h
//  p2_1116
//
//  Created by Cheng on 2017/11/16.
//  Copyright © 2017年 Cheng. All rights reserved.
//

#ifndef lock_h
#define lock_h

#include <mutex>
#include <condition_variable>
#include <mutex>
#include <thread>
using namespace std;

/*struct threadtable{
    thread::id id;
    thread t;
};
*/

class shared_mutex
{
private:
    mutex m_mutex;
    condition_variable m_con;
    bool ifWrite = false;
    size_t readCount = 0;
    bool read_cond() const{
        return (ifWrite==false);
    }
    bool write_cond() const{
        return ((ifWrite==false) && (readCount==0));
    }
    
public:
    void read()
    {
        unique_lock<mutex> m_lock(m_mutex);
        m_con.wait(m_lock, bind(&shared_mutex::read_cond, this));
        readCount++;
    }
    
    void unread()
    {
        unique_lock<mutex> m_lock(m_mutex);
        readCount--;
        m_con.notify_all();
    }
    
    void write()
    {
        unique_lock<mutex> m_lock(m_mutex);
        m_con.wait(m_lock, bind([](const bool *is_w, const size_t *read_c) -> bool
                                {
                                    return false == *is_w && 0 == *read_c;
                                }, &ifWrite, &readCount));
        ifWrite = true;
    }
    
    void unwrite()
    {
        unique_lock<mutex> m_lock(m_mutex);
        ifWrite = false;
        m_con.notify_all();
    }
};

class read_lock{
private:
    shared_mutex *m_sm;
public:
    read_lock(shared_mutex &sm){
        m_sm = &sm;
        m_sm->read();
    }
    ~read_lock(){
        m_sm->unread();
    }
};

class write_lock{
private:
    shared_mutex *m_sm;
public:
    write_lock(shared_mutex &sm){
        m_sm = &sm;
        m_sm->write();
    }
    ~write_lock(){
        m_sm->unwrite();
    }
};


#endif /* lock_h */
