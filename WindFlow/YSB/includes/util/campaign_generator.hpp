/**************************************************************************************
 *  Copyright (c) 2019- Gabriele Mencagli
 *  
 *  This file is part of StreamBenchmarks.
 *  
 *  StreamBenchmarks is free software dual licensed under the GNU LGPL or MIT License.
 *  You can redistribute it and/or modify it under the terms of the
 *    * GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License, or
 *      (at your option) any later version
 *    OR
 *    * MIT License: https://github.com/ParaGroup/StreamBenchmarks/blob/master/LICENSE.MIT
 *  
 *  StreamBenchmarks is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Lesser General Public License for more details.
 *  You should have received a copy of the GNU Lesser General Public License and
 *  the MIT License along with WindFlow. If not, see <http://www.gnu.org/licenses/>
 *  and <http://opensource.org/licenses/MIT/>.
 **************************************************************************************
 */

#ifndef COMPAIGN_H
#define COMPAIGN_H

// include
#include <utility>
#include <iostream>
#include <unordered_map>

using namespace std;

// campaign_record struct
struct campaign_record
{
    unsigned long ad_id;
    unsigned long cmp_id;

    // constructor
    campaign_record() {}

    // destructor
    ~campaign_record() {}
};

class CampaignGenerator {
private:
    unsigned int adsPerCampaign;
    unsigned long **arrays;
    unordered_map<unsigned long, unsigned int> map;
    campaign_record *relational_table;

public:
    // constructor
    CampaignGenerator(unsigned int _adsPerCampaign=10): adsPerCampaign(_adsPerCampaign)
    {
        // create the arrays
        arrays = (unsigned long **) malloc(sizeof(unsigned long *) * N_CAMPAIGNS * adsPerCampaign);
        for(size_t i=0; i<N_CAMPAIGNS*adsPerCampaign; i++)
            arrays[i] = (unsigned long *) malloc(sizeof(unsigned long) * 2);
        // create the relational table
        relational_table = (campaign_record *) malloc(sizeof(campaign_record) * N_CAMPAIGNS * adsPerCampaign);
        // initialize the arrays and the relational table
        int value = 0;
        int value2 = 0;
        unsigned long ad_id, cmp_id;
        for (size_t k=0; k<N_CAMPAIGNS; k++) {
            cmp_id = (value2);
            value2++;
            for (size_t i=0; i<adsPerCampaign; i++) {
                ad_id = value;
                relational_table[(k*adsPerCampaign) + i].ad_id = ad_id;
                relational_table[(k*adsPerCampaign) + i].cmp_id = cmp_id;
                arrays[value][0] = 0;
                arrays[value][1] = ad_id;
                value++;
            }
        }
        // initialize the hashmap
        for (unsigned int k=0; k<(N_CAMPAIGNS * adsPerCampaign); k++) {
            ad_id = relational_table[k].ad_id;
            map.insert(pair<unsigned long, unsigned int>(ad_id, k));
        }
    }

    // destructor
    ~CampaignGenerator()
    {
        // delete the arrays
        for(size_t i=0; i<N_CAMPAIGNS*adsPerCampaign; i++)
            delete arrays[i];
        delete arrays;
        // delete the relational table
        delete relational_table;
    }

    // get number of ads per campaign
    unsigned int getAdsCompaign() const
    {
        return adsPerCampaign;
    }

    // get a pointer to the relational table
    campaign_record *getRelationalTable() const
    {
        return relational_table;
    }

    // get a pointer to the arrays
    unsigned long **getArrays() const
    {
        return arrays;
    }

    // get a reference to the hashmap
    unordered_map<unsigned long, unsigned int> &getHashMap()
    {
        return map;
    }
};

#endif
