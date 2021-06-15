#pragma once
#include "../../utils/Config.h"
#include "Settings.h"
#include <stdlib.h>

class Relation
{        
public:
    enum class Type
    {
        LEFT,
        RIGHT
    };
    Relation(){};
    Relation(Type type);
    Relation(Type type, Tuple_t *data);

    void generate_range_partioned_data();
    inline Tuple_t *get_tuple(size_t index) { return &data[index]; };
    size_t get_size() { return size; };
    Type get_type() { return type; };
    void set_size(size_t size) { this->size = size; };
    void load_from_file(const char* fileName);
    void load_from_complete_file(const char* fileName);
    void dump_to_file(const char* fileName);

private:
    Type type;
    Tuple_t *data;
    size_t size;
    uint64_t key_start;
    uint64_t value_start;
    uint64_t key_stop = 0;

    struct FK_Generator {
        uint64_t start, end;
        uint64_t curr;
        FK_Generator(uint64_t start, uint64_t end): start(start), end(end), curr(start) {}
        Tuple_t operator()() {
            Tuple_t r = {curr,curr};
            curr = (curr==end)? start : curr+1;    // --> range from start ... end-1
            return r;
        }
    };

    struct PK_Generator {
        uint64_t curr;
        PK_Generator(uint64_t start) : curr(start) {}
        Tuple_t operator()() {
            Tuple_t r = {curr,curr};
            ++curr;
            return r;
        }
    };
};

Relation::Relation(Type type) : type(type)
{
    if (type == Type::LEFT)
    {
        if(Settings::NODE_ID == Settings::NODES_TOTAL-1) {
            size = Settings::LEFT_REL_FULL_SIZE - (Settings::NODES_TOTAL-1) * (Settings::LEFT_REL_FULL_SIZE / Settings::NODES_TOTAL);
        } else if (Settings::NODE_ID == 0) {
            size = Settings::LEFT_REL_FULL_SIZE / Settings::NODES_TOTAL;
        } else {
            size = Settings::LEFT_REL_FULL_SIZE / Settings::NODES_TOTAL;
        }
        Settings::LEFT_REL_SIZE = size;
        key_start = Settings::NODE_ID * (Settings::LEFT_REL_FULL_SIZE / Settings::NODES_TOTAL);
        value_start = Settings::NODE_ID * (Settings::LEFT_REL_FULL_SIZE / Settings::NODES_TOTAL);
    }
    else
    {
        if(Settings::NODE_ID == Settings::NODES_TOTAL-1) {
            size = Settings::RIGHT_REL_FULL_SIZE / Settings::NODES_TOTAL;
        } else if (Settings::NODE_ID == 0) {
            size = Settings::RIGHT_REL_FULL_SIZE - (Settings::NODES_TOTAL-1) * (Settings::RIGHT_REL_FULL_SIZE / Settings::NODES_TOTAL);
        } else {
            size = Settings::RIGHT_REL_FULL_SIZE / Settings::NODES_TOTAL;
        }
        Settings::RIGHT_REL_SIZE = size;
        key_start = (Settings::NODES_TOTAL - Settings::NODE_ID - 1) * (Settings::LEFT_REL_FULL_SIZE / Settings::NODES_TOTAL);
        key_stop = key_start + Settings::LEFT_REL_FULL_SIZE / Settings::NODES_TOTAL;
        value_start = Settings::NODE_ID * (Settings::RIGHT_REL_FULL_SIZE / Settings::NODES_TOTAL);
    }
};

Relation::Relation(Type type, Tuple_t *data) : type(type), data(data)
{
};

void Relation::load_from_complete_file(const char* fileName) {
	data = (Tuple_t*) malloc(sizeof(Tuple_t) * size);

	auto myfile = std::ifstream(fileName, std::ios::in | std::ios::binary);
    if (!myfile.good())
    {
        std::cout << "Could not open file: " << fileName << std::endl;
        exit(1);
    }
    myfile.seekg(sizeof(Tuple_t) * key_start, myfile.beg);
    myfile.read((char*)data, size * sizeof(Tuple_t));
    myfile.close();
}

void Relation::load_from_file(const char* fileName) {
	data = (Tuple_t*) malloc(sizeof(Tuple_t) * size);

	auto myfile = std::ifstream(fileName, std::ios::in | std::ios::binary);
    myfile.read((char*)&data[0], size * sizeof(Tuple_t));
    myfile.close();
}

void Relation::dump_to_file(const char* fileName) {
	
	auto myfile = std::fstream(fileName, std::ios::out | std::ios::binary);
    myfile.write((char*)&data[0], size * sizeof(Tuple_t));
    myfile.close();
}

void Relation::generate_range_partioned_data()
{
    std::cout << (type == Type::LEFT ? "Left" : "Right") << " relation, start key: " << key_start << " size: " << size << " key modulo: " << key_stop << '\n';

    data = (Tuple_t*) malloc(sizeof(Tuple_t) * size);
    if (type == Type::LEFT)
    {
        std::generate_n(data, size, PK_Generator{(uint64_t)key_start});
    }
    else
    {
        std::generate_n(data, size, FK_Generator{(uint64_t)key_start, (uint64_t)key_stop});
    }

    if (Settings::RANDOM_ORDER)
    {
        std::cout << "Randomizing tuple order in " << (type == Type::LEFT ? "left" : "right") << " relation..." << '\n';
        std::random_shuffle(data, data+size);
    }

}

