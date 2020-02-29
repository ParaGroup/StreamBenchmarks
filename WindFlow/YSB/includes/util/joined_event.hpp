/** 
 *  @file    joined_event.hpp
 *  @author  Gabriele Mencagli
 *  @date    14/08/2019
 *  
 *  @brief Structure of a joined tuple
 *  
 *  This file defines the structure of the tuples emitted by the joiner operator.
 *  The data type joined_event_t must be default constructible, with a copy constructor
 *  and copy assignment operator, and it must provide and implement the setInfo() and
 *  getInfo() methods.
 */

#ifndef YSB_JOINED_EVENT_HPP
#define YSB_JOINED_EVENT_HPP

#include <windflow.hpp>

using namespace std;

// joined_event_t struct
struct joined_event_t
{
    unsigned long ts; // timestamp
    unsigned long ad_id; // advertisement id
    unsigned long relational_ad_id;
    unsigned long cmp_id; // campaign id

    // constructor
    joined_event_t() {}

    // destructor
    ~joined_event_t() {}

    // getControlFields method (needed by the WindFlow library)
    tuple<size_t, uint64_t, uint64_t> getControlFields() const
    {
        return tuple<size_t, uint64_t, uint64_t>((size_t) cmp_id, (uint64_t) 0, (uint64_t) ts); // be careful with this cast!
    }

    // setControlFields method (needed by the WindFlow library)
    void setControlFields(size_t _key, uint64_t _id, uint64_t _ts)
    {
        cmp_id = (long) _key;
        ts = (long) _ts;
    }
};

#endif //YSB_JOINED_EVENT_HPP
