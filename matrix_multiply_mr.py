from mrjob.job import MRJob
from mrjob.step import MRStep

class MRMatrixMultiply(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
        ]

    def mapper(self, _, line):
        # Split the line with matrix name, row, column, and value
        matrix_name, row, column, value = line.split(',')
        row, column, value = int(row), int(column), float(value)

        if matrix_name == 'A':
            for k in range(100):  # assuming matrix B has 100 columns
                yield (row, k), ('A', column, value)
        elif matrix_name == 'B':
            for i in range(100):  # assuming matrix A has 100 rows
                yield (i, column), ('B', row, value)

    def reducer(self, key, values):
        A = {}
        B = {}
        for matrix, pos, value in values:
            if matrix == 'A':
                A[pos] = value
            else:
                B[pos] = value
        sum_product = sum(A[k] * B.get(k, 0) for k in A)
        yield key, sum_product

if __name__ == '__main__':
    MRMatrixMultiply.run()
