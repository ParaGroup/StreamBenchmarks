#pragma once

#include "filter_tuple.hpp"
#include "score_tuple.hpp"
#include "scorer_map.hpp"
#include <array>
#include <windflow.hpp>

namespace voip_stream {

class Score
{
public:

    Score();

    void operator ()(const FilterTuple &tuple, ScoreTuple &result, wf::RuntimeContext &rc);

private:

    template <typename Values>
    double sum(const Values &values, const std::array<double, 3> &weights) const
    {
        double sum = 0.0;

        for (int i = 0; i < 3; ++i) {
            sum += (values[i] * weights[i]);
        }

        return sum;
    }

private:

    ScorerMap scorer_map_;
    std::array<double, 3> weights_;
};

}
