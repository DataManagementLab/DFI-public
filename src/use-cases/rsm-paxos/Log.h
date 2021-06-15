/**
 * @file Log.h
 * @author jskrzypczak
 * Quick-and-dirty represnatation of a command log. It is kept in-memory.
 * @date 2020-07-01
 */

#pragma once

#include <algorithm>
#include <cstdint>
#include <vector>

#include "Settings.h"

class Log
{
private:
    size_t committed = 0;
    size_t highest_set = 0;
    size_t gapless_until = 0;

    std::vector<Command> log;
public:
    Log(size_t);
    ~Log();
    void set_slot(size_t, Command);
    Command get_slot(size_t);
    void set_committed(size_t);
};

Log::Log(size_t logsize) : log(logsize, EMPTY_CMD)
{
}

Log::~Log()
{
}

void Log::set_slot(size_t slotid, Command cmd)
{
    log[slotid] = cmd;
    while (log[gapless_until+1].operation != Operation::EMPTY)
    {
       gapless_until++;
    }
    highest_set = std::max(slotid, highest_set);
}

Command Log::get_slot(size_t slotid)
{
    return log[slotid];
}

void Log::set_committed(size_t pos) {
    committed = std::max(pos, committed);
}
