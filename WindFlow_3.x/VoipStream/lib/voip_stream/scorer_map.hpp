#pragma once

#include <string>
#include <unordered_map>
#include <vector>

namespace voip_stream {

class ScorerMap
{
public:

    class Entry
    {
        friend class ScorerMap;

    public:

        void set(int src, double rate);
        double get(int src) const;
        bool is_full() const;
        int pos(int src) const;
        const std::vector<double> &get_values() const;

    private:

        Entry(const std::vector<int> &fields);

    private:

        const std::vector<int> &fields_;
        std::vector<double> values_;
    };

public:

    ScorerMap(std::vector<int> fields);
    std::unordered_map<std::string, Entry> &get_map();
    Entry new_entry() const;

public:

    static double score(double v1, double v2, double vi);

private:

    std::unordered_map<std::string, Entry> map_;
    std::vector<int> fields_;
};

}
