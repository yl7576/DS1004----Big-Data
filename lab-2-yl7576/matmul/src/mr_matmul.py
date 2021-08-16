#! /usr/bin/env python

from mrjob.job import MRJob
from mrjob.compat import jobconf_from_env


class MRMatrixMultiplication(MRJob):
    """
    Multiply two matrices A and B together, returning a result C:

    A * B = C
    """

    def configure_args(self):
        super().configure_args()

        self.add_passthru_arg('--result-colsize',
                              required=True,
                              help=('Result matrix\'s number of columns.'
                                    ' Required.'),
                              type=int)

        self.add_passthru_arg('--result-rowsize',
                              required=True,
                              help=('Result matrix\'s number of rows.'
                                    ' Required.'),
                              type=int)

    def mapper(self, _, line):

        """
        Parameters:
            -: None
                A value parsed from input and by default it is None because the input is just raw text.
                We do not need to use this parameter.
            line: str
                each single line a file with newline stripped

            Yields:
                (key, value) 
                key: str
                value: int
                You have to design the intermediate keys and values shape yourself,
                and then use them appropriately in the reducer.
        """

        READING_FROM_A = False
        READING_FROM_B = False

        # find out what file we're currently parsing
        current_file = jobconf_from_env('mapreduce.map.input.file')
        if 'A.txt' in current_file:
            READING_FROM_A = True
        elif 'B.txt' in current_file:
            READING_FROM_B = True
        else:
            raise RuntimeError('Could not determine input file!')

        number_of_rows = self.options.result_rowsize  # C row count
        number_of_cols = self.options.result_colsize  # C column count

        # use line to yield necessary key-value pairs

        #yield key, value

        if READING_FROM_A == True:
            l = [int(i) for i in line.split(',')[:-1]] + [float(line.split(',')[-1])]
            keys = [(l[0],j) for j in range(number_of_cols)]
            for j in range(number_of_cols):
                yield (keys[j], (l[1], l[2])) # key = keys[j], value = (column_index, value)

        if READING_FROM_B == True:
            l = [int(i) for i in line.split(',')[:-1]] + [float(line.split(',')[-1])]
            keys = [(i, l[1]) for i in range(number_of_rows)]
            for i in range(number_of_rows):
                yield (keys[i], (l[0], l[2])) # key = keys[i], value = (row_index, value)


    # optional: implement the combiner:
    # def combiner(self, key, values):
        # start using the key-value pairs to calculate the result matrix
        # pass

    def reducer(self, key, values):


        """
        Implement your reducer here!

        This will accept a (key, list(values)) and calculate the result
        matrix C.
        
        Parameters:
            key: str
                same as the key defined in the mapper
            values: list
                list containing values corresponding to the key defined in the mapper

            Yields:
                key: str
                    same as the key defined in the mapper
                value: int
                    value corresponding to each key. This is the resulting matrix C
        """
        dict = {}
        for val in values:
            if val[0] in dict.keys():
                dict[val[0]] = dict[val[0]] * val[1]
            else:
                dict[val[0]] = val[1]
        yield (key, sum(dict.values()))

        pass

# don't forget the '__name__' == '__main__' block!
if __name__ == '__main__':
    MRMatrixMultiplication.run()
