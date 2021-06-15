/**
 * @file Filehelper.h
 * @author cbinnig, lthostrup, tziegler
 * @date 2018-08-17
 */

#ifndef DFI_SRC_UTILS_FILEHELPER_H_
#define DFI_SRC_UTILS_FILEHELPER_H_

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

namespace dfi {

class Filehelper {
 public:

// trim from start (in place)
  static void ltrim(std::string &s) {
    s.erase(
        s.begin(),
        std::find_if(s.begin(), s.end(),
                     std::not1(std::ptr_fun<int, int>(std::isspace))));
  }

  static size_t hash(string line, int keyPos) {
    string value = splitLine(keyPos, line);
    std::hash<std::string> hash_fn_str;
    size_t hash = hash_fn_str(value);
    return hash;
  }

  static vector<string> createFileSchema(string file, int numOfPart) {
    vector<string> outFiles;
    string remove = ".tbl";
    file = getFileName(file);
    std::string::size_type i = file.find(remove);
    if (i != std::string::npos)
      file.erase(i, remove.length());
    for (int i = 1; i <= numOfPart; i++) {
      stringstream ss;
      ss << file << "_" << i << ".tbl";
      outFiles.push_back(ss.str());
    }

    return outFiles;
  }

  static vector<string> createFileSchemaFullPath(string file, int numOfPart,
                                                 bool isHash) {
    vector<string> outFiles;
    string remove = ".tbl";
    std::string::size_type i = file.find(remove);
    if (i != std::string::npos)
      file.erase(i, remove.length());
    for (int i = 1; i <= numOfPart; i++) {
      stringstream ss;
      if (isHash) {
        ss << file << "_hash_" << i << ".tbl";
      } else {
        ss << file << "_roundR_" << i << ".tbl";
      }
      outFiles.push_back(ss.str());
      cout << ss.str() << endl;
    }

    return outFiles;
  }

  static string getFileName(const string& s) {

    char sep = '/';
    size_t i = s.rfind(sep, s.length());
    if (i != string::npos) {
      return (s.substr(i + 1, s.length() - i));
    }

    return ("");
  }

  static bool fileExists(const std::string& file) {
    struct stat buffer;
    return (stat(file.c_str(), &buffer) == 0);
  }

  static bool isDirectory(string dir) {
    struct stat sb;

    if (stat(dir.c_str(), &sb) == 0 && S_ISDIR(sb.st_mode)) {
      return true;
    }
    return false;
  }

  static string splitLine(int keyPos, string line) {
    string value = "";
    int index = 0;
    std::stringstream lineStream(line);
    while (std::getline(lineStream, value, '|')) {
      if (keyPos == index) {
        return value;
      }
      ++index;
    }
    return value;
  }

  static int extractVarChar(string type) {
    std::string::size_type start_position = 0;
    std::string::size_type end_position = 0;

    start_position = type.find("(");
    if (start_position != std::string::npos) {
      ++start_position;  // start after the double quotes.
      // look for end position;
      end_position = type.find(")");
      if (end_position != std::string::npos) {
        return stoi(type.substr(start_position, end_position - start_position));
      }
    }
    return 0;
  }

  static int countLineNumbers(string inFile) {
    ifstream file(inFile);
    string line = "";
    int index = 0;
    while (file.good()) {
      getline(file, line);
      ++index;
    }
    //counts last line
    file.close();
    return index - 1;
  }

};
}

#endif /* DFI_SRC_UTILS_FILEHELPER_H_ */
