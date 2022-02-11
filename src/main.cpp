
#include "main.h"

FileMerger::FileMerger(int thread, std::string dir, uint32_t memory):
    m_numThread(thread), m_directory(dir), m_memory(memory), m_index(0){
}

FileMerger::~FileMerger(){
    m_threadpool.clear();
}

/*
 *@brief it spawns the worker threads
 * */
void FileMerger::spawnWorkers(){
    for(auto i=0; i < m_numThread; i++){
        Worker *worker = new Worker(m_directory, m_memory);
        std::shared_ptr<ConcurrentQueue<std::string>> queue(new ConcurrentQueue<std::string>());
        std::shared_ptr<std::thread> th_obj(new std::thread(&Worker::entrypoint, worker, std::ref(*queue)));
        th_obj->detach();
        m_threadpool.push_back(std::make_pair(th_obj, queue));
    }
}

/*
 *It load balances different files to worker threads in round robin fashion
 **/
void FileMerger::loadbalanceFiles(){
    while(m_files.size() >= 2){
        if(m_index >= m_threadpool.size())
            m_index = 0;

        auto queue = m_threadpool[m_index].second;

        std::string file1 = m_files.front();
        m_files.pop_front();
        std::string file2 = m_files.front();
        m_files.pop_front();

        queue->push(file1);
        queue->push(file2);

        m_index++;
    }
}

/*
 *@brief starting point of the server. It spawns the worker threads and scans the directory.
 *It also load balances the files to different worker threads
 * */
void FileMerger::start(){
    spawnWorkers();
    sleep(5); //waiting for the worker thread to start
    while(1){
        getAllfiles();
        if(m_files.size() == 0){
            printf("No files to be merged\n");
        }

        if(m_files.size() >= 2){
            loadbalanceFiles();        
        } else if (m_files.size() == 1){
            auto file = m_files.front();
            if(file.find("INTER_") != std::string::npos){
                std::string full_path = m_directory + "/" + file;
                std::string final_file = m_directory + "/" + "MultiplexedFile.txt"; 
                rename(full_path.c_str(), final_file.c_str());
            }
        }

        if(m_files.size() == 1){
           m_files.pop_front();
           printf("Merge done\n");     
        }

        sleep(5);
    }
}

/*
 *@brief this function scans the directory and finds all files and stores the file name in a vector
 * */
void FileMerger::getAllfiles(){
    struct dirent *dir;
    DIR *d = opendir(m_directory.c_str());
    std::string fullpath;
    std::list<std::string> tmp_files;
    if(d){
        while ((dir = readdir(d)) != NULL)
        {
            //Condition to check regular file.
            if(dir->d_type==DT_REG){
                std::string d_name = dir->d_name;
                if(d_name.find(".processing") != std::string::npos || 
                        d_name.find(".tmp") != std::string::npos){
                    continue;
                }
                tmp_files.push_back(d_name);
            }
        }
        closedir(d);
    }

    m_files = tmp_files;
}

void printUsage(){
    printf("Usage: ./filemerger -t <NUM_WORKER> -m <memory limit in bytes> -d <directory to be scanned>\n");
    printf("Example: ./filemerger -t 4 -m 1024 -d ./data\n");
    printf("       -t   --job          Number of worker threads\n"
            "       -m   --memory       Memory limit\n"
            "       -d   --directory    directory to be scanned\n");
}

/*
 *@brief the main function
 *@param number of arguments
 *@param argument array
 * */
int main(int argc, char** argv){
    int next_option;
    int num_thread = 0;
    std::string dir("");
    uint32_t memory = 0; //in bytes
    const char* const short_options = "ht:m:d:";
    const struct option long_options[] = {
        { "help",            0, NULL, 'h' },
        { "memory",          1, NULL, 'm' },
        { "job",             1, NULL, 't' },
        { "directory",       1, NULL, 'd' },
        { NULL,              0, NULL,  0  }
    };

    while((next_option = getopt_long (argc, argv, short_options, long_options, NULL)) != -1){
        switch (next_option){
            case 'h':
                printUsage();
                break;
            case 't':
                num_thread = atoi(optarg);
                break;
            case 'm':
                memory = atol(optarg);
                break;
            case 'd':
                dir = optarg;
                break;
            default:
                printf("Unexpected error while parsing options!\n");
                return 1;
        }
    }while (next_option >= 0);


    if(argc < 7){
        printUsage();
        return 1;
    }
    
    size_t len = dir.length();
    if(dir[len-1] == '/'){
        dir = dir.substr(0, len-1); //removing right / from directory if any
    }

    std::shared_ptr<FileMerger> fmerger(new FileMerger(num_thread, dir, memory));
    fmerger->start();

    return 0;
}
