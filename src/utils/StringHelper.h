/**
 * @file StringHelper.h
 * @author cbinnig, tziegler
 * @date 2018-08-17
 */

#ifndef DFI_SRC_UTILS_STRINGHELPER_H_
#define DFI_SRC_UTILS_STRINGHELPER_H_

#include <string>
#include <vector>

using namespace std;

namespace dfi {

class StringHelper {
 public:
  static vector<string> split(string value, string sep = ",") {
    vector<string> values;
    char* cvalue = new char[value.length() + 1];
    strcpy(cvalue, value.c_str());
    char* token = strtok(cvalue, sep.c_str());

    while (token) {
      values.push_back(token);
      token = strtok(nullptr, sep.c_str());
    }

    delete[] cvalue;
    return values;
  }


  static void splitPerf(const string& value, std::vector<string>& retValues ,string sep = ",") {
      char* cvalue = new char[value.length() + 1];
      strcpy(cvalue, value.c_str());
      char* token = strtok(cvalue, sep.c_str());

      while (token) {
        retValues.emplace_back(token);
        token = strtok(nullptr, sep.c_str());
    }
    delete[] cvalue;
  }

};
}

#endif /* DFI_SRC_UTILS_STRINGHELPER_H_ */
