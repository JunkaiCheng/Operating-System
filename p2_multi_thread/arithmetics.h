#ifndef SRC_ARITHMETICS_H
#define SRC_ARITHMETICS_H

#include "query.h"
#include "data_query.h"

// Here follows the basic CURD Queries:
// C - Create (insert) an entry
// U - Update (existing query)
// R - Read   (read existsing queries, implemented as dump)
// D - Delete (delete existing query)

class MinQuery:public ComplexQuery {
	static constexpr const char* qname = "MIN";
public:
	using ComplexQuery::ComplexQuery;
	QueryResult::Ptr execute() override;
	std::string toString() override;
};

class MaxQuery :public ComplexQuery {
	static constexpr const char* qname = "MAX";
public:
	using ComplexQuery::ComplexQuery;
	QueryResult::Ptr execute() override;
	std::string toString() override;
};

class SumQuery :public ComplexQuery {
	static constexpr const char* qname = "SUM";
public:
	using ComplexQuery::ComplexQuery;
	QueryResult::Ptr execute() override;
	std::string toString() override;
};

class AddQuery :public ComplexQuery {
	static constexpr const char* qname = "ADD";
public:
	using ComplexQuery::ComplexQuery;
	QueryResult::Ptr execute() override;
	std::string toString() override;
};

class SubQuery :public ComplexQuery {
	static constexpr const char* qname = "SUB";
public:
	using ComplexQuery::ComplexQuery;
	QueryResult::Ptr execute() override;
	std::string toString() override;
};
#endif //SRC_ARITHMETICS_H
