#ifndef _WORKER_H_
#define _WORKER_H_
#include <list>
#include <sstream>
#include <string.h>
#include <iostream>
#include <chrono>
#include <algorithm>
#include <locale>
#include <fstream>
#include <thread>
#include <memory>

#include "ConcurrentQueue.h"

const std::string WHITESPACE = " \n\r\t\f\v";
const std::string HEADER = "Symbol, Timestamp, Price, Size, Exchange, Type";
typedef struct TradeInfo{
    std::string    symbol;
    uint64_t       timestamp;
    std::string    timestamp_str;
    std::string    price;
    std::string    size;
    std::string    exchange;
    std::string    type;
    TradeInfo(){
        symbol = "";
        timestamp = 0;
        timestamp_str = "";
        price = "";
        size = "";
        exchange = "";
        type = "";
    }
}TradeInfoT;

class Worker{
    public:
        Worker(const std::string dir, uint32_t memory);
        ~Worker();
        Worker(const Worker&) = delete;
        void entrypoint(ConcurrentQueue<std::string>&);
    private:
        std::string  m_directory;
        uint32_t     m_memory;
        uint32_t     m_maxLine;
        std::string ltrim(const std::string& s);
        std::string rtrim(const std::string& s);
        std::string trim(const std::string& s);
        void processFiles(std::string file1, std::string file2);
        void readFile(std::string&, std::string&, int);
        bool isContainSymbol(std::string line);
        void sortAndMerge(std::list<TradeInfoT> &, std::list<TradeInfoT> &, bool all,
                bool header, std::string& filename);
        void write(std::list<TradeInfoT> &records, bool header, std::string& file_name);
        std::string getfilename();
        bool parse(std::string& line, std::string& file_name, bool symbol, TradeInfoT& info);
        uint64_t getEpoch(std::string tstamp);
};

#endif
