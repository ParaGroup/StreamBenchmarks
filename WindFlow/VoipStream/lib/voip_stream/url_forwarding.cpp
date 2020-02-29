#include "url_forwarding.hpp"
#include "constants.hpp"
#include "util/log.hpp"

namespace voip_stream { namespace forwarding {

URL::URL()
    : scorer_map_({SOURCE_ENCR, SOURCE_ECR})
{}

void URL::operator ()(const FilterTuple &tuple, wf::Shipper<FilterTuple> &shipper, wf::RuntimeContext &rc)
{
    DEBUG_LOG("url_forwarding::tuple " << tuple);

    // forward the tuples from FoFiR
    if (tuple.source == SOURCE_FoFiR) {
        shipper.push(tuple);
        return;
    }

    auto key = tuple.cdr.calling_number + ':' + std::to_string(tuple.cdr.answer_timestamp);
    auto &map = scorer_map_.get_map();
    auto it = map.find(key);

    if (it != map.end()) {
        auto &entry = it->second;
        entry.set(tuple.source, tuple.rate);

        if (entry.is_full()) {
            // calculate the score for the ratio
            double ratio = entry.get(SOURCE_ENCR) / entry.get(SOURCE_ECR);
            double score = ScorerMap::score(URL_THRESHOLD_MIN, URL_THRESHOLD_MAX, ratio);

            FilterTuple result;
            result.ts = tuple.ts;
            result.source = SOURCE_URL;
            result.cdr = tuple.cdr;
            result.rate = score;
            shipper.push(result);

            map.erase(key);
        }
    } else {
        ScorerMap::Entry entry = scorer_map_.new_entry();
        entry.set(tuple.source, tuple.rate);
        map.insert(std::make_pair(key, entry));
    }
}

}}
