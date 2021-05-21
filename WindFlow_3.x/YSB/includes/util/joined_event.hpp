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
    unsigned long ad_id; // advertisement id
    unsigned long relational_ad_id;
    unsigned long cmp_id; // campaign id
    uint64_t ts;

    // Constructor I
	joined_event_t():
				   ad_id(0),
				   relational_ad_id(0),
				   cmp_id(0),
				   ts(0) {}

    // Constructor II
    joined_event_t(unsigned long _cmp_id, uint64_t _id)
    {
    	cmp_id = _cmp_id;
    }
};

#endif //YSB_JOINED_EVENT_HPP
