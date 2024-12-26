
/// Chernyshova Dana J4132

/// results: non-parallel t: 6.9074 sec. parallel t: 3.8088 sec.
/// how to reproduce: samples = 70_000, k = 100 (argv)
/// Each thread processes a subset of samples, computing distances and determining the nearest centroid.
/// Parallel reduction combines results from all threads to compute new centroids.

#include <iostream>
#include <vector>
#include <cmath>
#include <cstdlib>
#include <ctime>
#include <omp.h>
#include <limits>


struct Point {
    double x, y;

    Point(double x = 0, double y = 0) : x(x), y(y) {}

    Point operator+(const Point& other) const {
        return Point(x + other.x, y + other.y);
    }

    Point operator/(double value) const {
        return Point(x / value, y / value);
    }

    double distance(const Point& other) const {
        return std::sqrt((x - other.x) * (x - other.x) + (y - other.y) * (y - other.y));
    }
};

void kmeans_non_parallel(const std::vector<Point>& data, int k, int max_iterations, std::vector<int>& labels, std::vector<Point>& centroids) {
    int n = data.size();
    centroids.resize(k);
    for (int i = 0; i < k; ++i) {
        centroids[i] = data[std::rand() % n];
    }

    labels.resize(n);
    for (int iter = 0; iter < max_iterations; ++iter) {
        for (int i = 0; i < n; ++i) {
            double min_dist = std::numeric_limits<double>::max();
            for (int j = 0; j < k; ++j) {
                double dist = data[i].distance(centroids[j]);
                if (dist < min_dist) {
                    min_dist = dist;
                    labels[i] = j;
                }
            }
        }

        // upd step
        std::vector<Point> new_centroids(k, Point(0, 0));
        std::vector<int> counts(k, 0);
        for (int i = 0; i < n; ++i) {
            new_centroids[labels[i]] = new_centroids[labels[i]] + data[i];
            counts[labels[i]]++;
        }
        for (int j = 0; j < k; ++j) {
            if (counts[j] > 0) {
                centroids[j] = new_centroids[j] / counts[j];
            }
        }
    }
}

void kmeans_parallel(const std::vector<Point>& data, int k, int max_iterations, std::vector<int>& labels, std::vector<Point>& centroids) {
    int n = data.size();
    centroids.resize(k);
    for (int i = 0; i < k; ++i) {
        centroids[i] = data[std::rand() % n];
    }

    labels.resize(n);
    for (int iter = 0; iter < max_iterations; ++iter) {
#pragma omp parallel for
        for (int i = 0; i < n; ++i) {
            double min_dist = std::numeric_limits<double>::max();
            for (int j = 0; j < k; ++j) {
                double dist = data[i].distance(centroids[j]);
                if (dist < min_dist) {
                    min_dist = dist;
                    labels[i] = j;
                }
            }
        }

        // upd step
        std::vector<Point> new_centroids(k, Point(0, 0));
        std::vector<int> counts(k, 0);

#pragma omp parallel for reduction(+:new_centroids[:k], counts[:k])
        for (int i = 0; i < n; ++i) {
            new_centroids[labels[i]] = new_centroids[labels[i]] + data[i];
            counts[labels[i]]++;
        }
        for (int j = 0; j < k; ++j) {
            if (counts[j] > 0) {
                centroids[j] = new_centroids[j] / counts[j];
            }
        }
    }
}

void generate_data(std::vector<Point>& data, int n) {
    for (int i = 0; i < n; ++i) {
        data.emplace_back(std::rand() % 1000, std::rand() % 1000);
    }
}

int main(int argc, char* argv[]) {

    int num_samples = std::atoi(argv[1]);
    int k = std::atoi(argv[2]);
    int max_iterations = 100;

    std::srand(std::time(0));
    std::vector<Point> data;
    generate_data(data, num_samples);

    std::vector<int> labels;
    std::vector<Point> centroids;

    // non-parallel
    double start_time = omp_get_wtime();
    kmeans_non_parallel(data, k, max_iterations, labels, centroids);
    double end_time = omp_get_wtime();
    std::cout << "non-parallel t: " << (end_time - start_time) << " sec\n";

    // parallel
    start_time = omp_get_wtime();
    kmeans_parallel(data, k, max_iterations, labels, centroids);
    end_time = omp_get_wtime();
    std::cout << "parallel t: " << (end_time - start_time) << " sec\n";

    return 0;
}