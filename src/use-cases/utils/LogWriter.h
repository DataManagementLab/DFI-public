#pragma once
#include <string>
#include <functional>
#include <iostream>
#include <fstream>
#include <sstream>
#include <sys/stat.h>
#include <sstream>
#include <cctype>
#include <locale>
#include <algorithm>

using namespace std;

class Filehelper
{
public:
  // trim from start (in place)
  static void ltrim(std::string &s)
  {
    s.erase(
        s.begin(),
        std::find_if(s.begin(), s.end(),
                     std::not1(std::ptr_fun<int, int>(std::isspace))));
  }

  static string getFileName(const string &s)
  {

    char sep = '/';
    size_t i = s.rfind(sep, s.length());
    if (i != string::npos)
    {
      return (s.substr(i + 1, s.length() - i));
    }

    return ("");
  }

  static bool fileExists(const std::string &file)
  {
    struct stat buffer;
    return (stat(file.c_str(), &buffer) == 0);
  }

  static bool isDirectory(string dir)
  {
    struct stat sb;

    if (stat(dir.c_str(), &sb) == 0 && S_ISDIR(sb.st_mode))
    {
      return true;
    }
    return false;
  }

  static string splitLine(int keyPos, string line)
  {
    string value = "";
    int index = 0;
    std::stringstream lineStream(line);
    while (std::getline(lineStream, value, '|'))
    {
      if (keyPos == index)
      {
        return value;
      }
      ++index;
    }
    return value;
  }

  static int countLineNumbers(string inFile)
  {
    ifstream file(inFile);
    string line = "";
    int index = 0;
    while (file.good())
    {
      getline(file, line);
      ++index;
    }
    //counts last line
    file.close();
    return index - 1;
  }
};

class LogWriter
{
public:
  static void log(std::string filename, size_t size, size_t iter, size_t time_us, size_t servers, size_t threads, std::string strategy, double straggling_prob = 0, size_t sleep_us = 0)
  {
    if (!Filehelper::fileExists(filename))
    {
      std::ofstream log(filename, std::ios_base::app | std::ios_base::out);
      log << "Size,Iter,BW,Time,servers,threads,strategy,stragglingProb,sleepUs\n";
      log.flush();
    }

    std::ofstream log(filename, std::ios_base::app | std::ios_base::out);
    double time_ms = time_us / 1000;
    double time_s = time_ms / 1000;
    double total_size_mb = 1.0 * size * iter / 1024 / 1024;
    double bw = total_size_mb / time_s;
    log << size << "," << iter << "," << bw << "," << time_s << "," << servers << "," << threads << "," << strategy << "," << straggling_prob << "," << sleep_us << "\n";
  }



  static void latency_point(double latency)
  {
    latencies.push_back(latency);
  }

  static void latency_log(std::string filename, size_t size, size_t iter, size_t servers, size_t threads, std::string strategy)
  {
    if (!Filehelper::fileExists(filename))
    {
      std::ofstream log(filename, std::ios_base::app | std::ios_base::out);
      log << "Size,Iter,servers,threads,strategy,latAvg,lat95,lat99,latMin,latMax\n";
      log.flush();
    }

    std::ofstream log(filename, std::ios_base::app | std::ios_base::out);
    std::sort(latencies.begin(), latencies.end());
    double avg_lat = 0;
    for (double rtt : latencies)
		{
      avg_lat += rtt;
    }
    avg_lat = avg_lat / latencies.size();

    log << size << "," << iter << "," << "," << servers << "," << threads << "," << strategy << "," << avg_lat << "," << latencies[latencies.size()/2] << "," << latencies[95*latencies.size()/100] <<  
      "," << latencies[90*latencies.size()/100] << "," << latencies[0] << "," << latencies[latencies.size()-1] <<  "\n";
  }
private:
  static std::vector<double> latencies;
};