#include "management_query.h"
#include "db.h"
#include "query_results.h"

#include <iostream>

#include "lock.h"
#include <condition_variable>
#include <vector>
#include <thread>
extern int smp;
constexpr const char* LoadTableQuery::qname;
constexpr const char* DropTableQuery::qname;
constexpr const char* TruncateTableQuery::qname;
constexpr const char* DumpTableQuery::qname;
constexpr const char* ListTableQuery::qname;
constexpr const char* PrintTableQuery::qname;
constexpr const char* CopyTableQuery::qname;

using namespace std;

//extern std::mutex mutexLock;
extern shared_mutex mwr;
extern std::condition_variable cv;


QueryResult::Ptr CopyTableQuery::execute() {
    using namespace std;
    write_lock lck(mwr);
    Database& db = Database::getInstance();
    try {
        
        Table::Ptr newtable = nullptr;
        auto table=db[this->tableName];
        vector<string> field=table.field();
        newtable=std::make_unique<Table>(tableName, table.field());
        newtable->setName(newtableName);
        for (auto object : table)
        {
            vector<int> temp;
            for (int i=0; i< field.size(); ++i)
                //temp.insert(temp.end(), object[field[i]]);
                temp.push_back(object[field[i]]);
            newtable->insertByIndex(object.key(), temp);
        }
        db.registerTable(std::move(newtable));
    } catch (const std::exception& e) {
        return std::make_unique<ErrorMsgResult>(
                                                qname, e.what()
                                                );
    }
    --smp;
    cv.notify_all();
    return make_unique<NullQueryResult> ();
}

QueryResult::Ptr LoadTableQuery::execute() {
    
    std::ifstream infile(this->fileName);
    if (!infile.is_open()) {
        
        return std::make_unique<ErrorMsgResult>(
                                                qname, "Cannot open file '?'"_f % this->fileName
                                                );
    }
    Table::Ptr table = nullptr;
    try {
        write_lock lck(mwr);
        //        mutexLock.lock();
        table = loadTableFromStream(infile, this->fileName);
        Database& db = Database::getInstance();
        db.registerTable(std::move(table));
        //        mutexLock.unlock();
    } catch (const std::exception& e) {
        return std::make_unique<ErrorMsgResult>(qname, e.what());
    }
   vector<thread>::iterator it;
    /*for(it=threads.begin();it!=threads.end();)
    {
        thread::id temp=this_thread::get_id();
        if(it->get_id() ==temp)
        {
            thread t2(move (*it));
            threads.erase(it++);
            break;
        }
    }*/
 
    /*
    for (int m=0; m < threads.size(); ++m)
    {
        thread::id temp=this_thread::get_id();
        if(threads[m].get_id() ==temp)
            threads.erase(threads.begin()+m);
    }*/
    --smp;
    cv.notify_all();
    return std::make_unique<NullQueryResult> ();
}

std::string CopyTableQuery::toString() {
    return "QUERY = Copy TABLE, "
    "Table = \"" + this->tableName + "\", "
    "Newtable = \"" + this->newtableName + "\"" ;
}


std::string LoadTableQuery::toString() {
    return "QUERY = Load TABLE, FILE = \"" + this->fileName + "\"";
}



std::string DropTableQuery::toString() {
    return "QUERY = Drop TABLE, Table = \"" + this->tableName + "\"";
}

QueryResult::Ptr DropTableQuery::execute() {
    using namespace std;
    write_lock lck(mwr);
    Database& db = Database::getInstance();
    try {
        
        //        mutexLock.lock();
        db.dropTable(this->tableName);
        //        mutexLock.unlock();
    } catch (const std::invalid_argument& e) {
        return make_unique<ErrorMsgResult>(
                                           qname, string(e.what())
                                           );
    }
    --smp;
    cv.notify_all();
    return make_unique<NullQueryResult> ();
}

//Wei added(clear/truncate)
std::string TruncateTableQuery::toString() {
    return "QUERY = Truncate TABLE, Table = \"" + this->tableName + "\"";
}

QueryResult::Ptr TruncateTableQuery::execute() {
    using namespace std;
    write_lock lck(mwr);
    Database& db = Database::getInstance();
    try {
        
        //        mutexLock.lock();
        auto &table = db[this->tableName];
        //        mutexLock.unlock();
        table.clear();
    } catch (const std::invalid_argument& e) {
        return make_unique<ErrorMsgResult>(
                                           qname, string(e.what())
                                           );
    }
    --smp;
    cv.notify_all();
    return make_unique<NullQueryResult> ();
}

std::string DumpTableQuery::toString() {
    return "QUERY = Dump TABLE, "
    "Table = \"" + this->tableName + "\", "
    "ToFile = \"" + this->fileName + "\"" ;
}

QueryResult::Ptr DumpTableQuery::execute() {
    write_lock lck(mwr);
    std::ofstream ofile(this->fileName);
    if (!ofile.is_open()) {
        return std::make_unique<ErrorMsgResult> (
                                                 qname, tableName.c_str(),
                                                 R"(Cannot open File "?".)"_f % this->fileName
                                                 );
    }
    Database& db = Database::getInstance();
    try {
        
        ofile << (db[this->tableName]);
    } catch (const std::exception& e) {
        return std::make_unique<ErrorMsgResult>(
                                                qname, tableName.c_str()
                                                );
    }
    ofile.close();
    --smp;
    cv.notify_all();
    return std::make_unique<NullQueryResult> ();
}

QueryResult::Ptr ListTableQuery::execute() {
    read_lock lck(mwr);
    Database& db = Database::getInstance();
    db.printAllTable();
    --smp;
    cv.notify_all();
    return std::make_unique<SuccessMsgResult>(qname);
}

std::string ListTableQuery::toString() {
    return "QUERY = LIST";
}

QueryResult::Ptr PrintTableQuery::execute() {
    using namespace std;
    try {
        read_lock lck(mwr);
        Database &db = Database::getInstance();
        auto &table = db[this->tableName];
        cout << "================\n";
        cout << "TABLE = ";
        cout << table;
        cout << "================\n" << endl;
        --smp;
        cv.notify_all();
        return make_unique<NullQueryResult> ();
    } catch (const TableNameNotFound& e) {
        return make_unique<ErrorMsgResult> (
                                            qname, this->tableName.c_str(),
                                            "No such table."s
                                            );
    }
}

std::string PrintTableQuery::toString() {
    return "QUERY = SHOWTABLE, Table = \"" + this->tableName + "\"";
}


