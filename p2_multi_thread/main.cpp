#include <iostream>

#include "db_table.h"
#include "query_parser.h"
#include "query_builders.h"
#include "query.h"
#include "db.h"

#include "lock.h"

#include <vector>
#include <map>
//#include <pthread.h>
#include <thread>
#include <sstream>
#include <unistd.h>
#include <time.h>
#include <mutex>
#include <condition_variable>

using namespace std;
vector<thread> threads;



condition_variable cv;
shared_mutex mwr;
mutex mt;

int smp;

std::string extractQueryString() {
    std::string buf;
    do {
        int ch = cin.get();
        if (ch == ';') return buf;
        if (ch == EOF) throw std::ios_base::failure("End of input");
        buf.push_back(ch);
    } while(1);
}

void TryThread( Query::Ptr *query)
{
    QueryResult::Ptr result = (*query)->execute();
    if (result->success()) {
        cout << result->toString();
    } else {
        std::flush(cout);
        cerr << "QUERY FAILED:\n\t" << result->toString() << endl;
    }
}


int main() {
    //clock_t c;
    //c = clock();
    QueryParser p;
    smp=0;
    p.registerQueryBuilder(std::make_unique<QueryBuilder(MinTable)>());
    p.registerQueryBuilder(std::make_unique<QueryBuilder(MaxTable)>());
    p.registerQueryBuilder(std::make_unique<QueryBuilder(SumTable)>());
    p.registerQueryBuilder(std::make_unique<QueryBuilder(SubTable)>());
    p.registerQueryBuilder(std::make_unique<QueryBuilder(AddTable)>());
    p.registerQueryBuilder(std::make_unique<QueryBuilder(UpdateTable)>());
    p.registerQueryBuilder(std::make_unique<QueryBuilder(Debug)>());
    p.registerQueryBuilder(std::make_unique<QueryBuilder(ManageTable)>());
    p.registerQueryBuilder(std::make_unique<QueryBuilder(SELECT)>());
    p.registerQueryBuilder(std::make_unique<QueryBuilder(DUPLICATE)>());
    p.registerQueryBuilder(std::make_unique<QueryBuilder(SWAP)>());
    p.registerQueryBuilder(std::make_unique<QueryBuilder(COUNT)>());
    p.registerQueryBuilder(std::make_unique<QueryBuilder(Delete)>());
    p.registerQueryBuilder(std::make_unique<QueryBuilder(Insert)>());
    while (cin) {
        try {
            // A very standard REPL
            // REPL: Read-Evaluate-Print-Loop
            
            unique_lock <mutex> lk(mt);
            std::string queryStr = extractQueryString();
            Query::Ptr query;
            query= p.parseQuery(queryStr);
            while (smp==8)
                cv.wait(lk);
            ++smp;
           /* thread t1=std::thread([&]()
                                          {
                                              QueryResult::Ptr result = query->execute();
                                              if (result->success()) {
                                                  cout << result->toString();
                                              } else {
                                                  std::flush(cout);
                                                  cerr << "QUERY FAILED:\n\t" << result->toString() << endl;
                                              }
                                          });
            */
            
            thread t1(TryThread, &query);
            t1.join();

            
        } catch (const std::ios_base::failure& e) {
            // End of input
            break;
        } catch (const std::exception& e) {
            cerr << e.what() << endl;
        }
    }
    
    //    c = clock();
    //    c = clock() - c;
    //    cout << "Take me " <<  (float)c/CLOCKS_PER_SEC << " seconds\n";
    return 0;
}
