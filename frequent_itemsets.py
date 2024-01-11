from mrjob.job import MRJob
from itertools import combinations

class MRFrequentItemsets(MRJob):

    def configure_args(self):
        super(MRFrequentItemsets, self).configure_args()
        self.add_passthru_arg('--min_support', type=int, default=2, help='Minimum support threshold')

    def mapper(self, _, line):
        items = line.strip().split(',')
        # Generate all possible itemsets of size 1 and 2 from the transaction
        for item in items:
            yield (item, 1)
        
        for size in range(2, len(items) + 1):
            for itemset in combinations(sorted(items), size):
                yield (','.join(itemset), 1)

    def combiner(self, itemset, counts):
        yield (itemset, sum(counts))

    def reducer(self, itemset, counts):
        total_count = sum(counts)
        if total_count >= self.options.min_support:
            yield (itemset, total_count)

if __name__ == '__main__':
    MRFrequentItemsets.run()
