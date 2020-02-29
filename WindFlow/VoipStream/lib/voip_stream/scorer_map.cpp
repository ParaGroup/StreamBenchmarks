#include "scorer_map.hpp"
#include <cmath>

namespace voip_stream {

void ScorerMap::Entry::set(int src, double rate)
{
    values_.at(pos(src)) = rate;
}

double ScorerMap::Entry::get(int src) const
{
    return values_.at(pos(src));
}

bool ScorerMap::Entry::is_full() const
{
    for (double value : values_) {
        if (std::isnan(value)) {
            return false;
        }
    }
    return true;
}

int ScorerMap::Entry::pos(int src) const
{
    for (size_t i = 0; i < fields_.size(); i++) {
        if (fields_[i] == src) {
            return i;
        }
    }
    return -1;
}

const std::vector<double> &ScorerMap::Entry::get_values() const
{
    return values_;
}

ScorerMap::Entry::Entry(const std::vector<int> &fields)
    : fields_(fields)
    , values_(fields.size(), std::nan(""))
{}

ScorerMap::ScorerMap(std::vector<int> fields)
    : fields_(fields)
{}

std::unordered_map<std::string, ScorerMap::Entry> &ScorerMap::get_map()
{
    return map_;
}

ScorerMap::Entry ScorerMap::new_entry() const
{
    return Entry(fields_);
}

double ScorerMap::score(double v1, double v2, double vi)
{
    double score = vi / (v1 + (v2 - v1));
    if (score < 0) {
        score = 0;
    }
    if (score > 1) {
        score = 1;
    }
    return score;
}

}
