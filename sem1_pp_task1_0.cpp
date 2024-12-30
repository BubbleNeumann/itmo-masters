
/// Chernyshova Dana J4132

#include <iostream>
#include <string>
#include <cctype>

int countWords(const std::string& input) {
    int wordCount = 0;
    bool inWord = false;

    for (char c : input) {
        if (std::isspace(static_cast<unsigned char>(c))) {
            if (inWord) {
                inWord = false;
            }
        }
        else {
            if (!inWord) {
                inWord = true;
                ++wordCount;
            }
        }
    }

    return wordCount;
}

int main(int argc, char* argv[]) {
    std::string input = argv[1];
    int wordCount = countWords(input);
    std::cout << "num of words: " << wordCount << '\n';
    return 0;
}