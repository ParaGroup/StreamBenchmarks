#pragma once

namespace linear_road {

class AvgValue {
public:

    AvgValue(int initalValue);
    void update_average(int value);
    int get_average() const;

private:

    int sum_;
    int count_;
};

}
