# 6.1
from mrjob.job import MRJob
from mrjob.compat import jobconf_from_env
from itertools import product

class MRMatrixJoin(MRJob):
    def mapper(self, _, line):

        READING_FROM_track = False
        READING_FROM_artist_term = False

            # find out what file we're currently parsing
        current_file = jobconf_from_env('mapreduce.map.input.file')
        if 'artist_term.csv' in current_file:
            READING_FROM_artist_term = True
        elif 'track.csv' in current_file:
            READING_FROM_track= True
        else:
            raise RuntimeError('Could not determine input file!')

        if READING_FROM_track == True:
            l = line.split(',')
            artist_id = str(l[5])
            track_id = str(l[0])
            track_year = int(l[3])
            if track_year > 1990:
                yield artist_id, ('t', track_id)

        if READING_FROM_artist_term == True:
            l = line.split(',')
            artist_id = l[0]
            tag = l[1]
            yield artist_id, ('a', tag)

    def reducer(self, key, values):
        track_id = []
        tag = []
        for v in values:
            if v[0] == 't':
                track_id.append(v[1])
            else:
                tag.append(v[1])
        final_rows = list(product(track_id, tag))
        for i in final_rows:
            yield key, i

if __name__ == '__main__':
    MRMatrixJoin.run()

