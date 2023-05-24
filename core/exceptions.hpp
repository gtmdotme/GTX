//
// Created by zhou822 on 5/22/23.
//

#ifndef BWGRAPH_V2_EXCEPTIONS_HPP
#define BWGRAPH_V2_EXCEPTIONS_HPP
#include <iostream>
#include <exception>
namespace bwgraph{
class DeltaLockExecption: public std::exception
    {
        virtual const char* what() const throw()
        {
            return "Under mutual exclusion lock only current transaction should be able to modify the offset";
        }
    };
}
class TransactionTableOpCountException:public std::exception{
    virtual const char* what() const throw()
    {
        return "the operation count should never be negative";
    }
};
class TransactionTableMissingEntryException:public std::exception{
    virtual const char* what() const throw()
    {
        return "transaction entry should stay until being fully lazy updated";
    }
};
#endif //BWGRAPH_V2_EXCEPTIONS_HPP
