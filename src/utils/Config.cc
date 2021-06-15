

#include "Config.h"
#include "Logging.h"
#include <cmath>

using namespace dfi;


//DFI
string Config::DFI_REGISTRY_SERVER = "172.18.94.10";
// string Config::DFI_REGISTRY_SERVER = rdma::Config::getIP(rdma::Config::RDMA_INTERFACE);
uint16_t Config::DFI_REGISTRY_PORT = 7300;
uint16_t Config::DFI_REGISTRY_RDMA_MEM = 4096;
uint16_t Config::DFI_NODE_PORT = 7400;

bool Config::DFI_CLEANUP_FLOWS = false; // Set to true if application doesn't terminate after flow termination

bool Config::PRINT_SOURCE_STATS = true; // Prints out stats related to "flow-control", i.e. if a source is stalling to wait for targets

/**
 * @brief Config::DFI_NODES holds the IP adress of each node participating
 * NOTE: The index of the vector corresponds to nodeid - 1
 * i.e., each node gets a node id starting at 1 therefore the corresponding
 * IP is Config::DFI_NODES[(nodeid:1) - 1] => use the function
 * getIPFromNodeId(1) to retrieve the IP
 */
vector<string> Config::DFI_NODES = {"10.116.60.16:" + to_string(Config::DFI_NODE_PORT)};

size_t Config::DFI_SOURCE_SEGMENT_COUNT = 4;
size_t Config::DFI_FULL_SEGMENT_SIZE = (1024 * 64) + sizeof(DFI_SEGMENT_FOOTER_t);
size_t Config::DFI_SEGMENTS_PER_RING = 10;

/**
 * @brief Vector of available multicast addresses (in some systems, the multicast address must be a valid IP in the local network)
 */
vector<std::string> Config::DFI_MULTICAST_ADDRS = {
    "172.18.94.10", "172.18.94.11",
    "172.18.94.20", "172.18.94.21",
    "172.18.94.30", "172.18.94.31",
    "172.18.94.40", "172.18.94.41",
    "172.18.94.50", "172.18.94.51",
    "172.18.94.60", "172.18.94.61",
    "172.18.94.70", "172.18.94.71",
    "172.18.94.80", "172.18.94.81",
};


string Config::getBufferName(string &flowName, TargetID node_id)
{
  return flowName + to_string(node_id);
}

//SYSTEM
uint32_t Config::CACHELINE_SIZE = 64;

//LOGGING
int Config::LOGGING_LEVEL = 3;

string &Config::getIPFromNodeId(NodeID &node_id)
{
  if (node_id == 0 || node_id > Config::DFI_NODES.size())
  {
    Logging::error(__FILE__, __LINE__, "Requested an out-of-range NodeID from Config::DFI_NODES");
  }
  return Config::DFI_NODES.at(node_id - 1);
}
inline string trim(string str)
{
  str.erase(0, str.find_first_not_of(' '));
  str.erase(str.find_last_not_of(' ') + 1);
  return str;
}

void Config::init_vector(vector<string> &values, string csv_list)
{
  values.clear();
  char *csv_clist = new char[csv_list.length() + 1];
  strcpy(csv_clist, csv_list.c_str());
  char *token = strtok(csv_clist, ",");

  while (token)
  {
    values.push_back(token);
    token = strtok(nullptr, ",");
  }

  delete[] csv_clist;
}

void Config::init_vector(vector<int> &values, string csv_list)
{
  values.clear();
  char *csv_clist = new char[csv_list.length() + 1];
  strcpy(csv_clist, csv_list.c_str());
  char *token = strtok(csv_clist, ",");

  while (token)
  {
    string value(token);
    values.push_back(stoi(value));
    token = strtok(nullptr, ",");
  }

  delete[] csv_clist;
}

void Config::unload()
{
  google::protobuf::ShutdownProtobufLibrary();
}

void Config::load(const std::string& exec_path, const string &config_filename)
{
  string conf_file;
  if (exec_path.empty() || exec_path.find("/") == string::npos)
  {
    conf_file = ".";
  }
  else
  {
    conf_file = exec_path.substr(0, exec_path.find_last_of("/"));
  }
  if (config_filename.empty())
    conf_file += "/conf/DFI.conf";
  else
    conf_file += "/" + config_filename;
  

  ifstream file(conf_file.c_str());

  if (file.fail())
  {
    Logging::error(__FILE__, __LINE__, "Failed to load config file at " + conf_file + ". The default values are used.");
    return;
  }

  Logging::info("DFI Config file loaded: " + conf_file); 

  string line;
  string key;
  string value;
  int posEqual;
  while (getline(file, line))
  {
    if (line.length() == 0)
      continue;

    if (line[0] == '#')
      continue;
    if (line[0] == ';')
      continue;

    posEqual = line.find('=');
    key = line.substr(0, posEqual);
    value = line.substr(posEqual + 1);
    set(trim(key), trim(value));
  }
}

void Config::set(string key, string value)
{
  //config
  if (key.compare("DFI_REGISTRY_SERVER") == 0)
    Config::DFI_REGISTRY_SERVER = value;
  else if (key.compare("DFI_REGISTRY_PORT") == 0)
    Config::DFI_REGISTRY_PORT = stoi(value);
  else if (key.compare("DFI_NODES") == 0)
    init_vector(Config::DFI_NODES, value);
  else if (key.compare("LOGGING_LEVEL") == 0)
    Config::LOGGING_LEVEL = stoi(value);
  else if (key.compare("CACHELINE_SIZE") == 0)
    Config::CACHELINE_SIZE = stoi(value);
  else if (key.compare("DFI_INTERNAL_BUFFER_SIZE") == 0)
    Config::DFI_SOURCE_SEGMENT_COUNT = stoi(value);
  else if (key.compare("DFI_FULL_SEGMENT_SIZE") == 0)
    Config::DFI_FULL_SEGMENT_SIZE = stoi(value);
  else if (key.compare("DFI_SEGMENT_PAYLOAD_SIZE") == 0)
    Config::DFI_FULL_SEGMENT_SIZE = stoi(value) + sizeof(DFI_SEGMENT_FOOTER_t);
  else if (key.compare("DFI_SEGMENTS_PER_RING") == 0)
    Config::DFI_SEGMENTS_PER_RING = stoi(value);
}