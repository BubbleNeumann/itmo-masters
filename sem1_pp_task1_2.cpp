
/// Chernyshova Dana J4132

#include <iostream>
#include <vector>
#include <cstdlib>
#include <ctime>
#include <omp.h>

void multiply_matrices(const std::vector<std::vector<int>>& A,
    const std::vector<std::vector<int>>& B,
    std::vector<std::vector<int>>& C,
    int num_threads) {
    int N = A.size();

#pragma omp parallel for num_threads(num_threads) collapse(2)
    for (int i = 0; i < N; ++i) {
        for (int j = 0; j < N; ++j) {
            int sum = 0;
            for (int k = 0; k < N; ++k) {
                sum += A[i][k] * B[k][j];
            }
            C[i][j] = sum;
        }
    }
}

void initialize_matrix(std::vector<std::vector<int>>& matrix) {
    for (auto& row : matrix) {
        for (auto& elem : row) {
            elem = std::rand() % 100;
        }
    }
}

void print_matrix(const std::vector<std::vector<int>>& matrix) {
    for (const auto& row : matrix) {
        for (const auto& elem : row) {
            std::cout << elem << " ";
        }
        std::cout << std::endl;
    }
}

int main(int argc, char* argv[]) {

    int N = std::atoi(argv[1]);
    if (N <= 0) {
        std::cerr << "size must be a positive int" << std::endl;
        return 1;
    }

    std::srand(std::time(0));

    // init matrices
    std::vector<std::vector<int>> A(N, std::vector<int>(N));
    std::vector<std::vector<int>> B(N, std::vector<int>(N));
    std::vector<std::vector<int>> C(N, std::vector<int>(N, 0));

    initialize_matrix(A);
    initialize_matrix(B);

    double t1 = 0.0;

    for (int num_threads = 1; num_threads <= 10; ++num_threads) {
        double start_time = omp_get_wtime();

        multiply_matrices(A, B, C, num_threads);

        double end_time = omp_get_wtime();
        double elapsed_time = end_time - start_time;

        if (num_threads == 1) {
            t1 = elapsed_time; // time for single thread
        }

        double efficiency = t1 / elapsed_time;

        std::cout << "threads: " << num_threads << ", t: " << elapsed_time
            << " sec, efficiency: " << efficiency << std::endl;
    }

    // test correctness with 5x5 matrices
    if (N == 5) {
        std::cout << "A:" << std::endl;
        print_matrix(A);

        std::cout << "B:" << std::endl;
        print_matrix(B);

        std::cout << "C (res):" << std::endl;
        print_matrix(C);
    }

    return 0;
}