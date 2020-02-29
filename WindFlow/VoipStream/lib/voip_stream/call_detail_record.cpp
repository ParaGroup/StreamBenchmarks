#include "call_detail_record.hpp"
#include <charconv>
#include <iomanip>
#include <sstream>

namespace voip_stream {

CallDetailRecord CallDetailRecord::parse(const std::string &string)
{
    CallDetailRecord cdr{};

    for (std::size_t index = 0, start = 0, end = 0;
         start < string.size();
         start = end + 1, ++index)
    {
        if ((end = string.find(',', start)) == std::string::npos) {
            end = string.size();
        }

        auto first = string.data() + start;
        auto last = string.data() + end;

        // skip spaces
        while (*first == ' ') ++first;

        switch (index) {
        case 0:
            cdr.calling_number.assign(first, last);
            break;

        case 1:
            cdr.called_number.assign(first, last);
            break;

        case 2:
            {
                // parse the date-time (XXX ignore the time zone offset)
                std::istringstream stringstream(std::string(first, last));
                std::tm tm{};
                stringstream >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%s.");

                // parse the milliseconds part
                int millis = 0;
                std::from_chars(string.data() + string.find('.', start) + 1, last, millis);

                // convert everything to milliseconds
                std::time_t time = std::mktime(&tm);
                cdr.answer_timestamp = time * 1000 + millis;
                break;
            }

        case 3:
            std::from_chars(first, last, cdr.call_duration);
            break;

        case 4:
            cdr.call_established = string.compare(start, end - start, "true") == 0;
            break;
        }
    }

    return cdr;
}

}
