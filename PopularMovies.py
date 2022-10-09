from mrjob.job import MRJob
from mrjob.step import MRStep
 
class PopularMovies(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_movies_list, reducer=self.reducer_movies_list),
            MRStep(reducer=self.reducer_movies_sort),
                       
        ]
    
    def mapper_movies_list(self, _, line):
        userId, movie, rating, _ = line.split('	')

        yield movie, float(rating)


    def reducer_movies_list(self, key, values):
        items = list(values)
        movie = key
        avg = sum(items) / len(items) 
     

        yield None, (avg, key)

    def reducer_movies_sort(self, key, values):
        items = list(values)
        items.sort()

        top = items[len(items) - 1]
        xtop = items[0]

        

        yield (top[0], top[1]), (xtop[0], xtop[1])

if __name__ == '__main__':
    PopularMovies.run()