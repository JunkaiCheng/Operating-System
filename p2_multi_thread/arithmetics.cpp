#include "db.h"
#include "arithmetics.h"
#include"iostream"
#include "lock.h"
#include <condition_variable>
#include <vector>
#include <thread>
extern int smp;
extern shared_mutex mwr;
extern std::condition_variable cv;

constexpr const char* MinQuery::qname;
constexpr const char* MaxQuery::qname;
constexpr const char* SumQuery::qname;
constexpr const char* AddQuery::qname;
constexpr const char* SubQuery::qname;
//constexpr const char* UpdateQuery::qname;


//Min
QueryResult::Ptr MinQuery::execute() {
    
    using namespace std;
    if (this->operands.size() < 1)
        return make_unique<ErrorMsgResult>(
                                           qname, this->targetTable.c_str(),
                                           "Invalid number of operands (? operands)."_f % operands.size()
                                           );
    Database& db = Database::getInstance();
    read_lock lck(mwr);
    string field = operands[0];
    int min;
    int flag = 0;
    int flag2 = 0;
    int init = 0;
    cout << "ANSWER = ( ";
    for (int i = 0; i < this->operands.size(); ++i) {
        init = 0;
        flag = 0;
        field = operands[i];
        try {
            
            auto& table = db[this->targetTable];
            for (auto object : table) {
                if (init == 0) {
                    if (condition.size() == 0) {
                        min = object[field];
                        init++;
                        flag = 1;
                    }
                    else {
                        if (evalCondition(condition, object)) {
                            min = object[field];
                            init++;
                            flag = 1;
                        }
                        else continue;
                    }
                }
                if (condition.size() == 0) {
                    if (min > object[field]) {
                        min = object[field];
                    }
                }
                else {
                    if (evalCondition(condition, object)) {
                        if (min > object[field]) {
                            min = object[field];
                        }
                    }
                }
            }
            if (flag == 1) {
                cout << min << " ";
                flag2 = 1;
            }
        }//try
        
        catch (const TableNameNotFound& e) {
            return make_unique<ErrorMsgResult>(
                                               qname, this->targetTable.c_str(),
                                               "No such table."s
                                               );
        }
        catch (const TableFieldNotFound& e) {
            // Field not found
            return make_unique<ErrorMsgResult>(
                                               qname, this->targetTable.c_str(),
                                               e.what()
                                               );
        }
        catch (const IllFormedQueryCondition& e) {
            return std::make_unique<ErrorMsgResult>(
                                                    qname, this->targetTable.c_str(),
                                                    e.what()
                                                    );
        }
        catch (const invalid_argument& e) {
            // Cannot convert operand to string
            return std::make_unique<ErrorMsgResult>(
                                                    qname, this->targetTable.c_str(),
                                                    "Unknown error '?'"_f % e.what()
                                                    );
        }
        catch (const exception& e) {
            return std::make_unique<ErrorMsgResult>(
                                                    qname, this->targetTable.c_str(),
                                                    "Unkonwn error '?'."_f % e.what()
                                                    );
        }
    }
    if (flag2 == 1) cout << ") \n";
    --smp;
    cv.notify_all();
    return make_unique<NullQueryResult >();
}

std::string MinQuery::toString() {
    return "QUERY = MIN " + this->targetTable + "\"";
}

//Max
QueryResult::Ptr MaxQuery::execute() {
    using namespace std;
    read_lock lck(mwr);
    if (this->operands.size() < 1)
        return make_unique<ErrorMsgResult>(
                                           qname, this->targetTable.c_str(),
                                           "Invalid number of operands (? operands)."_f % operands.size()
                                           );
    Database& db = Database::getInstance();
    string field = operands[0];
    int max;
    int flag = 0;
    int flag2 = 0;
    int init = 0;
    cout << "ANSWER = ( ";
    for (int i = 0; i < this->operands.size(); ++i) {
        init = 0;
        flag = 0;
        field = operands[i];
        try {
            
            //int newValue = std::stoi(operands[1]);
            auto& table = db[this->targetTable];
            
            for (auto object : table) {
                if (init == 0) {
                    if (condition.size() == 0) {
                        max = object[field];
                        init++;
                        flag = 1;
                    }
                    else {
                        if (evalCondition(condition, object)) {
                            max = object[field];
                            init++;
                            flag = 1;
                        }
                        else continue;
                    }
                }
                if (condition.size() == 0) {
                    if (max < object[field]) {
                        max = object[field];
                    }
                }
                else {
                    if (evalCondition(condition, object)) {
                        if (max < object[field]) {
                            max = object[field];
                        }
                    }
                }
            }
            if (flag == 1) {
                cout << max << " ";
                flag2 = 1;
            }
        }//try
        
        catch (const TableNameNotFound& e) {
            return make_unique<ErrorMsgResult>(
                                               qname, this->targetTable.c_str(),
                                               "No such table."s
                                               );
        }
        catch (const TableFieldNotFound& e) {
            // Field not found
            return make_unique<ErrorMsgResult>(
                                               qname, this->targetTable.c_str(),
                                               e.what()
                                               );
        }
        catch (const IllFormedQueryCondition& e) {
            return std::make_unique<ErrorMsgResult>(
                                                    qname, this->targetTable.c_str(),
                                                    e.what()
                                                    );
        }
        catch (const invalid_argument& e) {
            // Cannot convert operand to string
            return std::make_unique<ErrorMsgResult>(
                                                    qname, this->targetTable.c_str(),
                                                    "Unknown error '?'"_f % e.what()
                                                    );
        }
        catch (const exception& e) {
            return std::make_unique<ErrorMsgResult>(
                                                    qname, this->targetTable.c_str(),
                                                    "Unkonwn error '?'."_f % e.what()
                                                    );
        }
    }
    if (flag2 == 1)cout << ") \n";
    --smp;
    cv.notify_all();
    return make_unique<NullQueryResult >();
}

std::string MaxQuery::toString() {
    return "QUERY = MAX " + this->targetTable + "\"";
}

//Sum
QueryResult::Ptr SumQuery::execute() {
    using namespace std;
    read_lock lck(mwr);
    if (this->operands.size() < 1)
        return make_unique<ErrorMsgResult>(
                                           qname, this->targetTable.c_str(),
                                           "Invalid number of operands (? operands)."_f % operands.size()
                                           );
    Database& db = Database::getInstance();
    try{
        
        auto &table=db[this->targetTable];
        vector<int> s;
        for (int i=0; i<operands.size(); i++)
            s.push_back(0);
        for (auto object : table)
        {
            if (evalCondition(condition, object))
            {
                for (int i=0; i<operands.size(); ++i)
                    s[i]=s[i]+object[operands[i]];
            }
        }
        cout << "ANSWER = (";
        for (int i=0; i<operands.size(); ++i)
            cout <<" "<<s[i];
        cout << " ) \n";
        --smp;
        cv.notify_all();
        return make_unique<NullQueryResult >();
        
    }catch (const TableNameNotFound& e) {
        return make_unique<ErrorMsgResult>(
                                           qname, this->targetTable.c_str(),
                                           "No such table."s
                                           );
    }
    catch (const TableFieldNotFound& e) {
        // Field not found
        return make_unique<ErrorMsgResult>(
                                           qname, this->targetTable.c_str(),
                                           e.what()
                                           );
    }
    catch (const IllFormedQueryCondition& e) {
        return std::make_unique<ErrorMsgResult>(
                                                qname, this->targetTable.c_str(),
                                                e.what()
                                                );
    }
    catch (const invalid_argument& e) {
        // Cannot convert operand to string
        return std::make_unique<ErrorMsgResult>(
                                                qname, this->targetTable.c_str(),
                                                "Unknown error '?'"_f % e.what()
                                                );
    }
    catch (const exception& e) {
        return std::make_unique<ErrorMsgResult>(
                                                qname, this->targetTable.c_str(),
                                                "Unkonwn error '?'."_f % e.what()
                                                );
    }
}

std::string SumQuery::toString() {
    return "QUERY = SUM " + this->targetTable + "\"";
}


//Add
QueryResult::Ptr AddQuery::execute() {
    using namespace std;
    write_lock lck(mwr);
    if (this->operands.size() < 2)
        return make_unique<ErrorMsgResult>(
                                           qname, this->targetTable.c_str(),
                                           "Invalid number of operands (? operands)."_f % operands.size()
                                           );
    Database& db = Database::getInstance();
    string dest = operands[operands.size() - 1];
    string field = operands[0];
    int counter = 0;
    try {
        
        auto& table = db[this->targetTable];
        for (auto object : table) {
            if (evalCondition(condition, object))
            {
                int r=0;
                for (int i=0; i < this->operands.size()-1; ++i)
                    r=r+object[operands[i]];
                object[operands[this->operands.size()-1]]=r;
                ++counter;
            }
        }
        --smp;
        cv.notify_all();
        return make_unique<RecordCountResult>(counter);
    }//try
    
    catch (const TableNameNotFound& e) {
        return make_unique<ErrorMsgResult>(
                                           qname, this->targetTable.c_str(),
                                           "No such table."s
                                           );
    }
    catch (const TableFieldNotFound& e) {
        // Field not found
        return make_unique<ErrorMsgResult>(
                                           qname, this->targetTable.c_str(),
                                           e.what()
                                           );
    }
    catch (const IllFormedQueryCondition& e) {
        return std::make_unique<ErrorMsgResult>(
                                                qname, this->targetTable.c_str(),
                                                e.what()
                                                );
    }
    catch (const invalid_argument& e) {
        // Cannot convert operand to string
        return std::make_unique<ErrorMsgResult>(
                                                qname, this->targetTable.c_str(),
                                                "Unknown error '?'"_f % e.what()
                                                );
    }
    catch (const exception& e) {
        return std::make_unique<ErrorMsgResult>(
                                                qname, this->targetTable.c_str(),
                                                "Unkonwn error '?'."_f % e.what()
                                                );
    }
}

std::string AddQuery::toString() {
    return "QUERY = ADD " + this->targetTable + "\"";
}

//Sub
QueryResult::Ptr SubQuery::execute() {
    using namespace std;
    write_lock lck(mwr);
    if (this->operands.size() < 2)
        return make_unique<ErrorMsgResult>(
                                           qname, this->targetTable.c_str(),
                                           "Invalid number of operands (? operands)."_f % operands.size()
                                           );
    Database& db = Database::getInstance();
    string dest = operands[operands.size() - 1];
    string minuend= operands[operands.size() - 2];
    string field = operands[0];
    int counter = 0;
    
    
    try {
        
        auto& table = db[this->targetTable];
        for (auto object : table) {
            if (evalCondition(condition, object))
            {
                int r=0;
                auto c=object[operands[0]];
                for (int i=1; i < this->operands.size()-1; ++i)
                    r=r+object[operands[i]];
                r=c-r;
                object[operands[this->operands.size()-1]]=r;
                ++counter;
            }
            
        }
        --smp;
        cv.notify_all();
        return make_unique<RecordCountResult>(counter);
    }//try
    
    catch (const TableNameNotFound& e) {
        return make_unique<ErrorMsgResult>(
                                           qname, this->targetTable.c_str(),
                                           "No such table."s
                                           );
    }
    catch (const TableFieldNotFound& e) {
        // Field not found
        return make_unique<ErrorMsgResult>(
                                           qname, this->targetTable.c_str(),
                                           e.what()
                                           );
    }
    catch (const IllFormedQueryCondition& e) {
        return std::make_unique<ErrorMsgResult>(
                                                qname, this->targetTable.c_str(),
                                                e.what()
                                                );
    }
    catch (const invalid_argument& e) {
        // Cannot convert operand to string
        return std::make_unique<ErrorMsgResult>(
                                                qname, this->targetTable.c_str(),
                                                "Unknown error '?'"_f % e.what()
                                                );
    }
    catch (const exception& e) {
        return std::make_unique<ErrorMsgResult>(
                                                qname, this->targetTable.c_str(),
                                                "Unkonwn error '?'."_f % e.what()
                                                );
    }
    
    
}

std::string SubQuery::toString() {
    return "QUERY = SUB " + this->targetTable + "\"";
}

