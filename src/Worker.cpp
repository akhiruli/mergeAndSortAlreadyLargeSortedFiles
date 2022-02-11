#include <pthread.h>

#include "Worker.h"

Worker::Worker(const std::string dir, uint32_t memory) : m_directory(dir), m_memory(memory){
    m_maxLine = m_memory/128; //assuming each line contains maximum of 128 bytes
}

Worker::~Worker(){

}

/*
 *@brief left trimming of string
 *@param string to be trimmed
 * */
std::string Worker::ltrim(const std::string& s){
    size_t start = s.find_first_not_of(WHITESPACE);
    return (start == std::string::npos) ? "" : s.substr(start);
}

/*
 *@brief right trimming of string
 *@param string to be trimmed
 * */
std::string Worker::rtrim(const std::string& s){
    size_t end = s.find_last_not_of(WHITESPACE);
    return (end == std::string::npos) ? "" : s.substr(0, end + 1);
}

/*
 *@brief trimming of string
 *@param string to be trimmed
 * */
std::string Worker::trim(const std::string& s){
    return rtrim(ltrim(s));
}

/*
 *@brief converting time in string to epoch time
 *@param time in string
 * */
uint64_t Worker::getEpoch(std::string tstamp){
    uint64_t time = 0;
    std::tm t = {};
    auto len = tstamp.find(".");
    std::string str;
    int milli = 0;

    if(len != std::string::npos){ 
        str = tstamp.substr(0, len);
        milli = stoi(tstamp.substr(len+1, tstamp.length()));
    } else{
        str = tstamp;
    }

    std::istringstream ss(str);
    if (ss >> std::get_time(&t, "%Y-%m-%d %H:%M:%S")){
        time = std::mktime(&t)*1000 + milli;
    }

    return time;
}

/*
 *@brief starting point of worker thread
 *@param Concurrent queue
 * */
void Worker::entrypoint(ConcurrentQueue<std::string>& queue){
    printf("Starting worker with ID %lu\n", pthread_self());
    while(1){
        auto file_name1 = queue.pop();
        auto file_name2 = queue.pop();
        processFiles(m_directory + "/" + file_name1, m_directory + "/" + file_name2);
    }
}

/*
 *@brief Starting point of processing two files
 *@param two sorted files to be merged
 * */
void Worker::processFiles(std::string file1, std::string file2){
    printf("Processing in the worker with ID %lu\n", pthread_self());
    std::string newfile1 = file1 + ".processing";
    std::string newfile2 = file2 + ".processing";

    if (rename(file1.c_str(), newfile1.c_str()) != 0){
        printf("Failed to rename file %s\n", file1.c_str());
        return;
    }
    if (rename(file2.c_str(), newfile2.c_str()) != 0){
        printf("Failed to rename file %s\n", file2.c_str());
        return;
    }

    //readFile(newfile1, newfile2, m_maxLine);
    readFile(newfile1, newfile2, 3);

    remove(newfile1.c_str());
    remove(newfile2.c_str());
}

/*
 *@brief generate file name for the merge result
 * */
std::string Worker::getfilename(){
    uint64_t ms = std::chrono::duration_cast<std::chrono::milliseconds >(std::chrono::system_clock::now().time_since_epoch()).count();
    char thread_id[64];
    sprintf(thread_id, "%lu", pthread_self());
    std::string file_name = m_directory + "/INTER_" + std::string(thread_id) + "_" + std::to_string(ms);
    return file_name;
}

/*
 *@brief It writes the info to resultant file
 *@param list of sorted records
 *@param True for header inclusion in the file and false for exclusion
 *@param file name where merged records to be written
 * */
void Worker::write(std::list<TradeInfoT> &records, bool header, std::string& file_name){
    std::list<TradeInfoT>::iterator itr = records.begin();
    std::ofstream outfile;
    outfile.open(file_name.c_str(), std::ios_base::app);
    if(header){
        outfile << HEADER;
    }
    while(itr != records.end()){
        TradeInfoT info = *itr;
        std::string line = "\n" + info.symbol + ", " + info.timestamp_str + ", "
            + info.price + ", " + info.size + ", " + info.exchange + ", " + info.type;
        outfile << line;
        ++itr;
    }

    outfile.close();
}

/*
 *@brief starting point of file record sort and merge
 *@param list of records from one file
 *@param list of records from the other file
 *@param true for all records to be written, false for  not
 *@param True for header inclusion in the file and false for exclusion
 *@param file name where merged records to be written
 * */
void Worker::sortAndMerge(std::list<TradeInfoT> &list1, std::list<TradeInfoT> &list2, bool all,
        bool header, std::string& filename){
    std::list<TradeInfoT> result;
    std::list<TradeInfoT>::iterator itr1 = list1.begin();
    std::list<TradeInfoT>::iterator itr2 = list2.begin();
    while(itr1 != list1.end() && itr2 != list2.end()){
        TradeInfoT first = *itr1;
        TradeInfoT second = *itr2;

        if(first.timestamp == second.timestamp){
            if( std::lexicographical_compare(first.symbol.begin(), first.symbol.end(),
                        second.symbol.begin(), second.symbol.end())){
                result.push_back(first);
                list1.pop_front();
                ++itr1;
            } else{
                result.push_back(second);
                list2.pop_front();
                ++itr2;
            }
        } else if(first.timestamp < second.timestamp){
            result.push_back(first);
            list1.pop_front();
            ++itr1;
        } else {
            result.push_back(second);
            list2.pop_front();
            ++itr2;
        }

    }

    if(all){
        while(itr1 != list1.end()){
            TradeInfoT first = *itr1;
            result.push_back(first);
            list1.pop_front();
            ++itr1;
        }

        while(itr2 != list2.end()){
            TradeInfoT second = *itr2;
            result.push_back(second);
            list2.pop_front();
            ++itr2;
        }
    }

    if(result.size()){
        write(result, header, filename);
    }
}

/*
 *@brief check if the records contain the symbol. This will be the case for intermediate file
 *@param one line of a file records
 *@return True if the symbol is present, False for not
 * */
bool Worker::isContainSymbol(std::string line){
    char *str = const_cast<char *>(line.c_str());
    char *saveptr = NULL;
    char *token = strtok_r(str, ",", &saveptr);
    if(token && strcmp(token, "Symbol") == 0)
        return true;

    return false;
}

/*
 *@brief Parsing one line records to different file
 *@param file name
 *@param it is out param which contains all the field of a record
 *@return True for parse success and false for failure
 * */
bool Worker::parse(std::string& line, std::string& file_name, bool symbol, TradeInfoT& info){
    if(line.empty())
        return false;
    char *saveptr = NULL;
    char *str = const_cast<char *>(line.c_str());
    char *token = strtok_r(str, ",", &saveptr);
    int count = 0;
    while(token != NULL){
        std::string field = trim(token);
        count++;
        switch (count){
            case 1:
                if(symbol){
                    info.symbol = field;
                } else{
                    std::string base_filename = file_name.substr(file_name.find_last_of("/\\") + 1);
                    std::string::size_type const p(base_filename.find_last_of('.'));
                    std::string file_without_extension = base_filename.substr(0, p);
                    info.symbol = file_without_extension;
                    info.timestamp = getEpoch(field);
                    info.timestamp_str = field;
                }
                break;
            case 2:
                if(symbol){
                    info.timestamp = getEpoch(field);
                    info.timestamp_str = field;
                } else{
                    info.price = field;
                }
                break;
            case 3:
                if(symbol){
                    info.price = field;
                } else {
                    info.size = field;
                }
                break;
            case 4:
                if(symbol){
                    info.size = field;
                } else {
                    info.exchange = field;
                }
                break;
            case 5:
                if(symbol){
                    info.exchange = field;
                }else{
                    info.type = field;
                }
                break;
            case 6:
                if(symbol){
                    info.type = field;
                }
                break;
            default:
                printf("No case matched\n");
        }
        token = strtok_r(NULL, ",", &saveptr);
    }

    return true;
}

/*
 *@brief reading files line by line
 *@param First file
 *@param Second file
 *@param muximum number of lines to be read from both the files
 * */
void Worker::readFile(std::string& file1, std::string& file2, int max_line){
    std::ifstream infile1;
    std::ifstream infile2;
    std::string file_name_orig = getfilename();
    std::string file_name = file_name_orig + ".tmp";

    infile1.open(file1.c_str(), std::ios::in);
    infile2.open(file2.c_str(), std::ios::in);
    std::string line1, line2;
    std::list<TradeInfoT> list1, list2;
    uint64_t count = 0;
    bool is_symbol1 = false;
    bool is_symbol2 = false;
    bool header = true;
    bool header_parsed = false;
    while(!infile1.eof() && !infile2.eof()) {
        std::getline(infile1, line1);
        std::getline(infile2, line2);
        if(!header_parsed){
            header_parsed = true;
            is_symbol1 = isContainSymbol(line1);        
            is_symbol2 = isContainSymbol(line2);
            continue;
        }
        count += 2;

        if(count >= 2){ //to avoid the header
            TradeInfoT info1, info2;
            bool ret1 = parse(line1, file1, is_symbol1, info1);
            bool ret2 = parse(line2, file2, is_symbol2, info2);
            if(ret1){
                list1.push_back(info1);
            }

            if(ret2){
                list2.push_back(info2);
            }
        }

        if(count == max_line){
            sortAndMerge(list1, list2, false, header, file_name);
            count = list1.size() + list2.size();
            header = false;
        }
    }

    while(!infile1.eof()){
        TradeInfoT info;
        std::getline(infile1, line1);
        bool ret = parse(line1, file1, is_symbol1, info);
        if(ret){
            list1.push_back(info);
            count++;
        }
        if(count == max_line){
            sortAndMerge(list1, list2, true, header, file_name);
            count = list1.size() + list2.size();
            header = false;
        }
    }

    while(!infile2.eof()){
        TradeInfoT info;
        std::getline(infile2, line2);
        bool ret = parse(line2, file2, is_symbol1, info);
        if(ret){
            list2.push_back(info);
            count++;
        }
        if(count == max_line){
            sortAndMerge(list1, list2, true, header, file_name);
            count = list1.size() + list2.size();
            header = false;
        }
    }

    if((list1.size() + list2.size()) > 0){
        sortAndMerge(list1, list2, true, header, file_name);
    }

    rename(file_name.c_str(), file_name_orig.c_str());
}
