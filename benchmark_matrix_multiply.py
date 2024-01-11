import time
import random
from matrix_multiply import matrix_multiply
from subprocess import call

def generate_matrix(rows, columns, seed=42):
    """Generates a matrix filled with random numbers using a given seed."""
    random.seed(seed)  # Ensures reproducible results
    return [[random.random() for _ in range(columns)] for _ in range(rows)]

def write_matrix_to_file(matrix, filename, matrix_name):
    """Writes a matrix to a file with the correct MapReduce format."""
    with open(filename, 'w') as f:
        for i, row in enumerate(matrix):
            for j, value in enumerate(row):
                f.write(f'{matrix_name},{i},{j},{value}\n')

def run_mapreduce_job():
    """Runs the MapReduce matrix multiplication job and suppresses the output."""
    with open('output_mr.txt', 'w') as outfile:
        call(['python', 'matrix_multiply_mr.py', '--runner', 'inline', 'input.csv'], stdout=outfile)

def benchmark(A, B):
    # Write matrices to files with correct format for MapReduce
    write_matrix_to_file(A, 'A.csv', 'A')
    write_matrix_to_file(B, 'B.csv', 'B')
    
    # Combine files for MapReduce input
    with open('input.csv', 'w') as outfile:
        for fname in ['A.csv', 'B.csv']:
            with open(fname) as infile:
                outfile.write(infile.read())

    # Benchmark traditional multiplication
    start_time = time.time()
    C_normal = matrix_multiply(A, B)
    normal_time = time.time() - start_time
    print(f"Normal matrix multiplication took {normal_time:.2f} seconds.")

    # Benchmark MapReduce multiplication
    start_time = time.time()
    run_mapreduce_job()
    mapreduce_time = time.time() - start_time
    print(f"MapReduce matrix multiplication took {mapreduce_time:.2f} seconds.")

# Dimensions for the matrices
m, n, p = 400, 400, 400  # Example dimensions for m x n and n x p matrices

# Generate matrices A (m by n) and B (n by p)
A = generate_matrix(m, n)  
B = generate_matrix(n, p)  

if __name__ == '__main__':
    benchmark(A, B)
