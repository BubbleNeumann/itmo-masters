
/// Chernyshova Dana J4132

#include <iostream>
#include <cstdlib>
#include <vector>
#include <omp.h>
#include <algorithm>
#include <ctime>

int main(int argc, char* argv[]) {

    int N = atoi(argv[1]);

    std::vector<int> vec(N);
    std::srand(std::time(0));
    for (int i = 0; i < N; ++i) {
        vec[i] = std::rand();
    }

    int max_value = INT_MIN; // to store the maximum value across all threads

    for (int num_threads = 1; num_threads <= 10; ++num_threads) {
        double start_time = omp_get_wtime();

        // each thread computes its own maximum
        int local_max = INT_MIN;

#pragma omp parallel for num_threads(num_threads) reduction(max:local_max)
        for (int i = 0; i < N; ++i) {
            local_max = std::max(local_max, vec[i]);
        }

        max_value = local_max;
        double end_time = omp_get_wtime();

        // results
        std::cout << "threads: " << num_threads << ", t: " << (end_time - start_time)
            << " sec, max val: " << max_value << std::endl;
    }

    // test the correctness of the program with 10 elements
    if (N == 10) {
        std::cout << "inp array: ";
        for (int i = 0; i < N; ++i) {
            std::cout << vec[i] << " ";
        }
        std::cout << std::endl;
        std::cout << "computed max val: " << max_value << std::endl;
        std::cout << "expected max val: " << *std::max_element(vec.begin(), vec.end()) << std::endl;
    }

    return 0;
}