#include "score.hpp"
#include "constants.hpp"
#include "util/log.hpp"

namespace voip_stream {

Score::Score()
    : scorer_map_({SOURCE_FoFiR, SOURCE_URL, SOURCE_ACD})
    , weights_({FOFIR_WEIGHT, URL_WEIGHT, ACD_WEIGHT})
{}

void Score::operator ()(const FilterTuple &tuple, ScoreTuple &result, wf::RuntimeContext &rc)
{
    DEBUG_LOG("score::tuple " << tuple);

    auto key = tuple.cdr.calling_number + ':' + std::to_string(tuple.cdr.answer_timestamp);
    auto &map = scorer_map_.get_map();
    auto it = map.find(key);

    if (it != map.end()) {
        auto &entry = it->second;

        if (entry.is_full()) {
            double main_score = sum(entry.get_values(), weights_);

            result.ts = tuple.ts;
            result.cdr = tuple.cdr;
            result.score = main_score;
        } else {
            entry.set(tuple.source, tuple.rate);

            result.ts = tuple.ts;
            result.cdr = tuple.cdr;
            result.score = 0;
        }
    } else {
        ScorerMap::Entry entry = scorer_map_.new_entry();
        entry.set(tuple.source, tuple.rate);
        map.insert(std::make_pair(key, entry));

        result.ts = tuple.ts;
        result.cdr = tuple.cdr;
        result.score = 0;
    }
}

}
