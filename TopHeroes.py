from mrjob.job import MRJob
from mrjob.step import MRStep
 
class TopHeroes(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_hero_list, reducer=self.reducer_hero_list),
            MRStep(reducer=self.reducer_hero_friends),
                       
        ]

    def mapper_hero_list(self, _, line):
        line= line.split()
        heroes= len(line) - 1
        hero = line[0]
        yield hero , heroes
  
    def reducer_hero_list(self, key, values):
        yield None, (sum(values), key)

    def reducer_hero_friends(self, key, values):
        items = list(values)
        items.sort()
        hero = items[-1]
        yield hero[1], hero[0]


if __name__ == '__main__':
    TopHeroes.run()