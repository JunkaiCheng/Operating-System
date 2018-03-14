#include "db.h"
#include "data_query.h"
#include <iostream>
#include <map>
#include <algorithm>
#include "lock.h"
#include <condition_variable>
#include <vector>
#include <thread>
extern int smp;
constexpr const char* InsertQuery::qname;
constexpr const char* UpdateQuery::qname;
constexpr const char* DeleteQuery::qname;
constexpr const char* SelectQuery::qname;
constexpr const char* SwapQuery::qname;
constexpr const char* CountQuery::qname;
constexpr const char* DuplicateQuery::qname;

extern shared_mutex mwr;
extern std::condition_variable cv;


QueryResult::Ptr DeleteQuery::execute() {
    using namespace std;
    write_lock lck(mwr);
    if (this->operands.size() != 0)
        return make_unique<ErrorMsgResult> (
                                            qname, this->targetTable.c_str(),
                                            "Invalid number of operands (? operands)."_f % operands.size()
                                            );
    Database& db = Database::getInstance();
    int counter = 0;
    try {
        
        auto& table = db[this->targetTable];
        for (auto it=table.begin(); it!=table.end();)
        {
            auto object=*it;
            if (evalCondition(condition, object))
            {
                it=table.erase(it);
                ++counter;
            }
            else ++it;
        }
        --smp;
        cv.notify_all();
        return make_unique<RecordCountResult>(counter);
    } catch (const TableNameNotFound& e) {
        return make_unique<ErrorMsgResult> (
                                            qname, this->targetTable.c_str(),
                                            "No such table."s
                                            );
    } catch (const TableFieldNotFound& e) {
        // Field not found
        return make_unique<ErrorMsgResult> (
                                            qname, this->targetTable.c_str(),
                                            e.what()
                                            );
    }  catch (const IllFormedQueryCondition& e) {
        return std::make_unique<ErrorMsgResult> (
                                                 qname, this->targetTable.c_str(),
                                                 e.what()
                                                 );
    } catch (const invalid_argument& e) {
        // Cannot convert operand to string
        return std::make_unique<ErrorMsgResult> (
                                                 qname, this->targetTable.c_str(),
                                                 "Unknown error '?'"_f % e.what()
                                                 );
    } catch (const exception& e) {
        return std::make_unique<ErrorMsgResult> (
                                                 qname, this->targetTable.c_str(),
                                                 "Unkonwn error '?'."_f % e.what()
                                                 );
    }
}

struct mapTable{
    std::string key;
    std::string value;
};

bool comp(const mapTable &a, const mapTable &b){
    return a.key < b.key;
}

QueryResult::Ptr SelectQuery::execute() {
    using namespace std;
    read_lock lck(mwr);
    if (this->operands[0]!="KEY")
        return make_unique<ErrorMsgResult> (qname, this->targetTable.c_str(), "Invalid operands)."_f % operands.size());
    Database& db = Database::getInstance();
    int counter = 0;
        try {
            
            vector<mapTable> mt;
            int numOpr=int(this->operands.size());
            auto& table = db[this->targetTable];
            for (auto object : table) {
                if (evalCondition(condition, object)) {
                    string output;
                    output= output+"( " + object.key();
                    for (int i=0; i<numOpr-1; ++i)
                    {
                        string op=operands[i+1];
                        output =output+" " + to_string(object[op]);
                    }
                    output=output+" )";
                    counter++;
                    mapTable newmt;
                    newmt.key=object.key();
                    newmt.value=output;
                    mt.push_back(newmt);
                }
            }
            sort(mt.begin(),mt.end(),comp);
            for (int j=0; j< mt.size();++j)
                cout << mt[j].value<<endl;
            cout << endl;
            --smp;
            cv.notify_all();
            return make_unique<NullQueryResult>();
    }catch (const TableNameNotFound& e) {
        return make_unique<ErrorMsgResult> (
                                            qname, this->targetTable.c_str(),
                                            "No such table."s
                                            );
    } catch (const TableFieldNotFound& e) {
        // Field not found
        return make_unique<ErrorMsgResult> (
                                            qname, this->targetTable.c_str(),
                                            e.what()
                                            );
    }  catch (const IllFormedQueryCondition& e) {
        return std::make_unique<ErrorMsgResult> (
                                                 qname, this->targetTable.c_str(),
                                                 e.what()
                                                 );
    } catch (const invalid_argument& e) {
        // Cannot convert operand to string
        return std::make_unique<ErrorMsgResult> (
                                                 qname, this->targetTable.c_str(),
                                                 "Unknown error '?'"_f % e.what()
                                                 );
    } catch (const exception& e) {
        return std::make_unique<ErrorMsgResult> (
                                                 qname, this->targetTable.c_str(),
                                                 "Unkonwn error '?'."_f % e.what()
                                                 );
    }
}


QueryResult::Ptr DuplicateQuery::execute() {
    using namespace std;
    write_lock lck(mwr);
    if (this->operands.size() != 0)
        return make_unique<ErrorMsgResult> (qname, this->targetTable.c_str(),"Invalid number of operands (? operands)."_f % operands.size() );
    Database& db = Database::getInstance();
    int counter = 0;
    try {
        
        auto& table = db[this->targetTable];
        vector<string> fieldTemp =table.field();
        int s=int(fieldTemp.size());
        int size=int(table.size());
        int flag=0;
        for (auto object : table) {
            if (evalCondition(condition, object)) {
                vector<int> data;
                for (int i=0; i<s; ++i)
                {
                    string fieldName=fieldTemp[i];
                    data.push_back(object[fieldName]);
                }
                string newKey=object.key()+"_copy";
                table.insertByIndex(newKey, data);
                ++counter;
            }
            ++flag;
            if (flag==size) break;
        }
        --smp;
        cv.notify_all();
        return make_unique<RecordCountResult>(counter);
    }catch (const TableNameNotFound& e) {
        return make_unique<ErrorMsgResult> (
                                            qname, this->targetTable.c_str(),
                                            "No such table."s
                                            );
    } catch (const TableFieldNotFound& e) {
        // Field not found
        return make_unique<ErrorMsgResult> (
                                            qname, this->targetTable.c_str(),
                                            e.what()
                                            );
    }  catch (const IllFormedQueryCondition& e) {
        return std::make_unique<ErrorMsgResult> (
                                                 qname, this->targetTable.c_str(),
                                                 e.what()
                                                 );
    } catch (const invalid_argument& e) {
        // Cannot convert operand to string
        return std::make_unique<ErrorMsgResult> (
                                                 qname, this->targetTable.c_str(),
                                                 "Unknown error '?'"_f % e.what()
                                                 );
    } catch (const exception& e) {
        return std::make_unique<ErrorMsgResult> (
                                                 qname, this->targetTable.c_str(),
                                                 "Unkonwn error '?'."_f % e.what()
                                                 );
    }
}

QueryResult::Ptr SwapQuery::execute() {
    using namespace std;
    write_lock lck(mwr);
    if (this->operands.size() != 2)
        return make_unique<ErrorMsgResult> (qname, this->targetTable.c_str(),"Invalid number of operands (? operands)."_f % operands.size() );
    Database& db = Database::getInstance();
    int counter = 0;
    try {
        
        string field1 = operands[0];
        string field2 = operands[1];
        auto& table = db[this->targetTable];
        for (auto object : table) {
            if (evalCondition(condition, object)) {
                int temp;
                temp=object[field1];
                object[field1] = object[field2];
                object[field2]=temp;
                counter++;
            }
        }
        --smp;
        cv.notify_all();
        return make_unique<RecordCountResult>(counter);
    }catch (const TableNameNotFound& e) {
        return make_unique<ErrorMsgResult> (
                                            qname, this->targetTable.c_str(),
                                            "No such table."s
                                            );
    } catch (const TableFieldNotFound& e) {
        // Field not found
        return make_unique<ErrorMsgResult> (
                                            qname, this->targetTable.c_str(),
                                            e.what()
                                            );
    }  catch (const IllFormedQueryCondition& e) {
        return std::make_unique<ErrorMsgResult> (
                                                 qname, this->targetTable.c_str(),
                                                 e.what()
                                                 );
    } catch (const invalid_argument& e) {
        // Cannot convert operand to string
        return std::make_unique<ErrorMsgResult> (
                                                 qname, this->targetTable.c_str(),
                                                 "Unknown error '?'"_f % e.what()
                                                 );
    } catch (const exception& e) {
        return std::make_unique<ErrorMsgResult> (
                                                 qname, this->targetTable.c_str(),
                                                 "Unkonwn error '?'."_f % e.what()
                                                 );
    }
}

QueryResult::Ptr CountQuery::execute() {
    using namespace std;
    read_lock lck(mwr);
    if (this->operands.size() != 0)
        return make_unique<ErrorMsgResult> (qname, this->targetTable.c_str(),"Invalid number of operands (? operands)."_f % operands.size());
    Database& db = Database::getInstance();
    int counter = 0;
    try{
        
        auto& table = db[this->targetTable];
        for (auto object : table) {
            if (evalCondition(condition, object)) {
                counter++;
            }
        }
        string output= "ANSWER = " + to_string(counter);
        std::cout << output<<endl;
        --smp;
        cv.notify_all();
        return make_unique<NullQueryResult>();
    }catch (const TableNameNotFound& e) {
        return make_unique<ErrorMsgResult> (
                                            qname, this->targetTable.c_str(),
                                            "No such table."s
                                            );
    } catch (const TableFieldNotFound& e) {
        // Field not found
        return make_unique<ErrorMsgResult> (
                                            qname, this->targetTable.c_str(),
                                            e.what()
                                            );
    }  catch (const IllFormedQueryCondition& e) {
        return std::make_unique<ErrorMsgResult> (
                                                 qname, this->targetTable.c_str(),
                                                 e.what()
                                                 );
    } catch (const invalid_argument& e) {
        // Cannot convert operand to string
        return std::make_unique<ErrorMsgResult> (
                                                 qname, this->targetTable.c_str(),
                                                 "Unknown error '?'"_f % e.what()
                                                 );
    } catch (const exception& e) {
        return std::make_unique<ErrorMsgResult> (
                                                 qname, this->targetTable.c_str(),
                                                 "Unkonwn error '?'."_f % e.what()
                                                 );
    }
}


QueryResult::Ptr UpdateQuery::execute() {
    using namespace std;
    write_lock lck(mwr);
    if (this->operands.size() != 2)
        return make_unique<ErrorMsgResult> (
                                            qname, this->targetTable.c_str(),
                                            "Invalid number of operands (? operands)."_f % operands.size()
                                            );
    Database& db = Database::getInstance();
    string field = operands[0];
    int counter = 0;
    try {
        
        int newValue = std::stoi(operands[1]);
        auto& table = db[this->targetTable];
        for (auto object : table) {
            if (evalCondition(condition, object)) {
                object[field] = newValue;
                counter++;
            }
        }
        --smp;
        cv.notify_all();
        return make_unique<RecordCountResult>(counter);
    } catch (const TableNameNotFound& e) {
        return make_unique<ErrorMsgResult> (
                                            qname, this->targetTable.c_str(),
                                            "No such table."s
                                            );
    } catch (const TableFieldNotFound& e) {
        // Field not found
        return make_unique<ErrorMsgResult> (
                                            qname, this->targetTable.c_str(),
                                            e.what()
                                            );
    }  catch (const IllFormedQueryCondition& e) {
        return std::make_unique<ErrorMsgResult> (
                                                 qname, this->targetTable.c_str(),
                                                 e.what()
                                                 );
    } catch (const invalid_argument& e) {
        // Cannot convert operand to string
        return std::make_unique<ErrorMsgResult> (
                                                 qname, this->targetTable.c_str(),
                                                 "Unknown error '?'"_f % e.what()
                                                 );
    } catch (const exception& e) {
        return std::make_unique<ErrorMsgResult> (
                                                 qname, this->targetTable.c_str(),
                                                 "Unkonwn error '?'."_f % e.what()
                                                 );
    }
}



QueryResult::Ptr InsertQuery::execute() {
    using namespace std;
    write_lock lck(mwr);
    Database& db = Database::getInstance();
    string key = operands[0];
    
    vector<int> tuple;
    int i;
    int value;
    
    
    for(i=1;i<operands.size();i++){
        value=std::stoi(operands[i]);
        tuple.push_back(value);
    }
    
    try {
        
        auto& table = db[this->targetTable];
        if (table.ifexist(key))
            return make_unique<NullQueryResult>();
        table.insertByIndex(key, tuple);
//        mutexLock.unlock();
        vector<thread>::iterator it;
        --smp;
        cv.notify_all();
        return std::make_unique<NullQueryResult>();
 
    } catch (const TableNameNotFound& e) {
        return make_unique<ErrorMsgResult> (
                                            qname, this->targetTable.c_str(),
                                            "No such table."s
                                            );
    } catch (const TableFieldNotFound& e) {
        // Field not found
        return make_unique<ErrorMsgResult> (
                                            qname, this->targetTable.c_str(),
                                            e.what()
                                            );
    }  catch (const IllFormedQueryCondition& e) {
        return std::make_unique<ErrorMsgResult> (
                                                 qname, this->targetTable.c_str(),
                                                 e.what()
                                                 );
    } catch (const invalid_argument& e) {
        // Cannot convert operand to string
        return std::make_unique<ErrorMsgResult> (
                                                 qname, this->targetTable.c_str(),
                                                 "Unknown error '?'"_f % e.what()
                                                 );
    } catch (const exception& e) {
        return std::make_unique<ErrorMsgResult> (
                                                 qname, this->targetTable.c_str(),
                                                 "Unkonwn error '?'."_f % e.what()
                                                 );
    }
}

std::string UpdateQuery::toString() {
    return "QUERY = UPDATE " + this->targetTable + "\"";
}

std::string SelectQuery::toString() {
    return "QUERY = SELECT " + this->targetTable + "\"";
}

std::string SwapQuery::toString() {
    return "QUERY = SWAP " + this->targetTable + "\"";
}

std::string CountQuery::toString() {
    return "QUERY = COUNT " + this->targetTable + "\"";
}

std::string DuplicateQuery::toString() {
    return "QUERY = DUPLICATE " + this->targetTable + "\"";
}

std::string DeleteQuery::toString() {
    return "QUERY = DELETE " + this->targetTable + "\"";
}


std::string InsertQuery::toString() {
    return "QUERY = INSERT " + this->targetTable + "\"";
}

