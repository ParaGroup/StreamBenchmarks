#pragma once

namespace voip_stream {

class VariableEWMA {
public:

    VariableEWMA(double age);
    void add(double value);
    double get_average() const;

private:

    static constexpr int WARMUP_SAMPLES = 10;

private:

    double decay_;
    double average_;
    int count_;
};

}
