#ifndef THREADPOOL_H
#define THREADPOOL_H

#include <list>
#include <pthread.h>
#include <exception>
#include <semaphore.h>
#include <../lock/locker.h>
#include "../CGImysql/sql_connection_pool.h"

template<typename T>
class threadpool
{
    public:
    threadpool(connection_pool *connPool,int thread_number=8,int max_requests=10000);
    ~threadpool();
    bool append(T *request);

    private:
    static void *worker(void *arg);
    void run();

    private:
    int m_thread_number;        //线程池中的线程数
    int m_max_requests;         //请求队列中允许的最大请求数
    pthread_t *m_threads;       //描述线程池的数组，其大小为m_thread_number
    std::list<T *> m_workqueue; //请求队列
    locker m_queuelocker;       //保护请求队列的互斥锁
    sem m_queuestat;            //是否有任务需要处理
    bool m_stop;                //是否结束线程
    connection_pool *m_connPool;  //数据库
}
template<typename T>
threadpool<T>::threadpool(connection_pool *connPool,int thread_number,int max_requests):m_thread_number(thread_number),m_max_requests(max_requests),m_connPool(connPool),m_stop(false),m_threads(NULL)
{
    if(m_thread_number <= 0||m_max_requests <= 0){
        throw std::exception("thread_number or max_requests is invalid");
    }
    m_threads = new pthread_t[m_thread_number];
    if(!m_threads){
        throw std::exception();
    }
    for(int i=0;i<m_thread_number;i++){
        if(pthread_create(&m_threads[i],NULL,worker,this) != 0){
            delete[] m_threads;
            throw std::exception("pthread_create failed");
        }
        if(pthread_detach(m_threads[i]) != 0){
            delete[] m_threads;
            throw std::exception("pthread_detach failed");
        }
    }
}

template<typename T>
threadpool<T>::~threadpool()
{
    delete[] m_threads;
    m_stop = true;
}

template<typename T>
bool threadpool<T>::append(T* request)
{
    m_queuelocker.lock();
    if(m_thread_number > m_max_requests){
        m_queuelocker.unlock();
        return false;
    }
    m_workqueue.push_back(request);
    m_queuelocker.unlock();
    m_queuestat.post();
    return true;
}

template<typename T>
void threadpool<T>::worker(void *arg)
{
    threadpool *pool = (threadpool *)arg;
    pool->run();
    return NULL;
}

template<typename T>
void threadpool<T>::run()
{
    while(!m_stop){
        m_queuestat.wait();
        m_queuelocker.lock();
        if(m_workqueue.empty())
        {
            m_queuelocker.unlock();
            continue;
        }
        T *request = m_workqueue.front();
        m_workqueue.pop_front();
        m_queuelocker.unlock();
        if(!request)
        {
            continue;
        }
        connectionRAII mysqlcon(&request->mysql, m_connPool);

        request->process();
    }
}


#endif