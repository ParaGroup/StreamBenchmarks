#include "acd.hpp"
#include "constants.hpp"
#include "util/log.hpp"

namespace voip_stream {

ACD::ACD()
    : avg_(0)
    , scorer_map_({SOURCE_CT24, SOURCE_ECR24})
{}

void ACD::operator ()(const FilterTuple &tuple, wf::Shipper<FilterTuple> &shipper, wf::RuntimeContext &rc)
{
    DEBUG_LOG("acd::tuple " << tuple);

    if (tuple.source == SOURCE_GlobalACD) {
        avg_ = tuple.rate;
    } else {
        auto key = tuple.cdr.calling_number + ':' + std::to_string(tuple.cdr.answer_timestamp);
        auto &map = scorer_map_.get_map();
        auto it = map.find(key);

        if (it != map.end()) {
            auto &entry = it->second;
            entry.set(tuple.source, tuple.rate);

            if (entry.is_full()) {
                // calculate the score for the ratio
                double ratio = (entry.get(SOURCE_CT24) / entry.get(SOURCE_ECR24)) / avg_;
                double score = ScorerMap::score(ACD_THRESHOLD_MIN, ACD_THRESHOLD_MAX, ratio);

                FilterTuple result;
                result.ts = tuple.ts;
                result.source = SOURCE_ACD;
                result.cdr = tuple.cdr;
                result.rate = score;
                shipper.push(std::move(result));

                map.erase(key);
            }
        } else {
            ScorerMap::Entry entry = scorer_map_.new_entry();
            entry.set(tuple.source, tuple.rate);
            map.insert(std::make_pair(key, entry));
        }
    }
}

}
