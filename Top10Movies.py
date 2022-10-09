from mrjob.job import MRJob
from mrjob.job import MRStep

class Top10Movies(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_movies_list, reducer=self.reducer_movies_list),
            MRStep(reducer=self.reducer_top_10),
                       
        ]
    
    def mapper_movies_list(self, _, line):
        userId, movie, rating, _ = line.split('	')

        yield movie, float(rating)


    def reducer_movies_list(self, key, values):
        items = list(values)
        movie = key
        avg = sum(items) / len(items) 
     

        yield None, (avg, key)

    def reducer_top_10(self, _, values):
        items = list(values)
        items.sort(reverse = True)

        for index, line in enumerate(items):
            yield line[0], line[1]
            if index == 9:
                break


if __name__ == '__main__':
    Top10Movies.run()
