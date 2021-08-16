# 6.2
from mrjob.job import MRJob
from mrjob.compat import jobconf_from_env
from mrjob.step import MRStep

class MRMatrixGroupby(MRJob):
    def steps(self):
            return [
                MRStep(mapper=self.mapper,
                       reducer=self.reducer1),
                MRStep(reducer=self.reducer2)
            ]

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

        if READING_FROM_artist_term == True:
            l = line.split(',')
            artist_id = l[0]
            tag = l[1]
            yield artist_id, ['a',tag]

        if READING_FROM_track == True:
            l = line.split(',')
            artist_id = l[-1]
            duration = l[4]
            year = l[3]
            yield artist_id, ['t', year, duration]

    def reducer1(self, key, values):
        year = []
        duration = []
        groupby_key_tag = []
        for v in values:
            if v[0] == 't':
                year.append(int(v[1]))
                duration.append(float(v[2]))
            if v[0] == 'a':
                groupby_key_tag.append(v[1])
        track_len = len(duration)
        for i in range(track_len):
            for k in groupby_key_tag:
                yield(k, [year[i], duration[i], key])


    def reducer2(self, key, values):
        year = []
        duration = []
        artist_id = []
        for v in values:
            year.append(v[0])
            duration.append(v[1])
            artist_id.append(v[2])
        min_year = min(year)
        count_id = len(artist_id)
        avg_duration = sum(duration)/len(duration)
        yield key, [min_year, avg_duration, count_id]

if __name__ == '__main__':
    MRMatrixGroupby.run()
