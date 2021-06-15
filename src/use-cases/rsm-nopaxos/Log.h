/**
 * @file Log.h
 * @author jskrzypczak
 * Quick-and-dirty representation of a command log. It is kept in-memory.
 * @date 2020-07-01
 */

#pragma once

#include "Settings.h"

#include <vector>

class Log
{
private:
    volatile uint64_t last_slot_filled = 0;

    std::vector<Command> log;
public:
    Log(uint64_t);
    ~Log();

    void append_command(Command);
    void set_command(uint64_t, Command);
    Command get_command(uint64_t);
    uint64_t get_next_slot_index();

};

Log::Log(uint64_t logsize) : log(logsize)
{
    log[0] = NOOP_CMD; // just to fill first slot
}

Log::~Log()
{
}

inline void Log::append_command(Command command)
{
    log[last_slot_filled + 1] = command;
    last_slot_filled++; // slot must incremented AFTER command is set
}

inline void Log::set_command(uint64_t slot, Command command)
{
    log[slot] = command;
}

inline Command Log::get_command(uint64_t slot)
{
    return log[slot];
}

inline uint64_t Log::get_next_slot_index()
{
    return last_slot_filled + 1;
}
