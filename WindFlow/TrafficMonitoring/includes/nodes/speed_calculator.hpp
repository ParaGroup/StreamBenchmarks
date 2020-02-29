/**
 *  @file    speed_calculator.hpp
 *  @author  Alessandra Fais
 *  @date    17/06/2019
 *
 *  @brief Node that evaluated the average speed of vehicles in each road of the city
 */

#ifndef TRAFFICMONITORING_SPEED_CALCULATOR_HPP
#define TRAFFICMONITORING_SPEED_CALCULATOR_HPP

#include <ff/ff.hpp>
#include "../util/result.hpp"
#include "../util/constants.hpp"

using namespace std;
using namespace ff;
using namespace wf;

/**
 *  @class Speed_Calculator_Functor
 *
 *  @brief Define the logic of the Speed Calculator
 */
class Speed_Calculator_Functor {
private:
    struct Road_Speed {
        int road_id;
        deque<double> road_speeds;
        size_t win_size;
        double current_sum;
        double incremental_average;
        double squared_sum;
        double incremental_variance;
        size_t count_absolute;

        Road_Speed(int _road_id, double _speed): road_id(_road_id), current_sum(_speed),
                incremental_average(_speed), squared_sum(_speed * _speed), incremental_variance(0.0),
                win_size(_road_win_size), count_absolute(0)
        {
            road_speeds.push_back(_speed);
        }

        void update_average_speed(double speed) {
            // control window size
            if (road_speeds.size() > win_size - 1) {
                current_sum -= road_speeds.at(0);
                road_speeds.pop_front();
            }

            // update average speed value
            if (road_speeds.size() == 1) {
                road_speeds.push_back(speed);
                current_sum += speed;
                incremental_average = current_sum / road_speeds.size();
                squared_sum += (speed * speed);
                incremental_variance = squared_sum - (road_speeds.size() * incremental_average * incremental_average);
            } else {
                double cur_avg = (current_sum + speed) / road_speeds.size() + 1;
                double cur_var = (squared_sum + speed * speed) - (road_speeds.size() + 1) * cur_avg * cur_avg;
                double standard_deviation = sqrt(cur_var / road_speeds.size() + 1);

                if (abs(speed - cur_avg) <= 2 * standard_deviation) {
                    road_speeds.push_back(speed);
                    current_sum += speed;
                    squared_sum += (speed * speed);
                    incremental_average = cur_avg;
                    incremental_variance = cur_var;
                }
            }
        }

        ~Road_Speed() {}
    };

    size_t processed;       // tuples counter
    unordered_map<int, Road_Speed> roads;

    // time variables
    unsigned long app_start_time;
    unsigned long current_time;

    // runtime information
    size_t parallelism;
    size_t replica_id;

public:

    /**
     *  @brief Constructor
     */
    Speed_Calculator_Functor(const unsigned long _app_start_time):
            processed(0),
            app_start_time(_app_start_time),
            current_time(_app_start_time) {}

    /**
     *  @brief Compute average speed for each road id
     *
     *  The current average speed value, computed for the road id, is written in the received tuple and
     *  forwarded to the sink (in-place computation).
     *  @param r tuple coming from the map matching module, containing actual vehicle speed and road id
     */
    void operator()(result_t& r, RuntimeContext& rc) {
        if (processed == 0) {
            parallelism = rc.getParallelism();
            replica_id = rc.getReplicaIndex();
        }
        //print_result("[SpeedCalculator] Received tuple: ", r);

        if (roads.find(r.key) == roads.end()) {
            Road_Speed rs(r.key, r.speed);
            roads.insert(make_pair(r.key, rs));
        } else {
            roads.at(r.key).update_average_speed(r.speed);
        }

        r.speed = roads.at(r.key).incremental_average;
        processed++;
        current_time = current_time_nsecs();
    }

    ~Speed_Calculator_Functor() {
        /*if (processed != 0) {
            cout << "[SpeedCalculator] replica " << replica_id + 1 << "/" << parallelism
                 << ", execution time: " << (current_time - app_start_time) / 1e09
                 << " s, processed: " << processed
                 << ", bandwidth: " << processed / ((current_time - app_start_time) / 1e09)
                 << endl;
        }*/
    }
};

#endif //TRAFFICMONITORING_SPEED_CALCULATOR_HPP
