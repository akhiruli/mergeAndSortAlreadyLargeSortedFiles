#ifndef _MAIN_H_
#define _MAIN_H_

#include <string.h>
#include <dirent.h>
#include <getopt.h>
#include <stdio.h>

#include "Worker.h"
class FileMerger{
    public:
        using ThreadPool = std::vector<std::pair<std::shared_ptr<std::thread>, std::shared_ptr<ConcurrentQueue<std::string>>>>;
        FileMerger(int thread, std::string dir, uint32_t memory);
        ~FileMerger();
        FileMerger(const FileMerger&) = delete;

        void start();
    private:
        int         m_numThread;
        std::string m_directory;
        uint32_t    m_memory;
        int         m_index;
        std::list<std::string> m_files;
        ThreadPool  m_threadpool;

        void loadbalanceFiles();
        void getAllfiles();
        void spawnWorkers();
};
#endif
