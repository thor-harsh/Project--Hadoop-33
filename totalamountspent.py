from mrjob.job import MRJob

class MRFriendsByAge(MRJob):

    def mapper(self,_,line):
        (Id,name,age,numFriends)=line.split('\t')
        yield age,numFriends

    def reducer(self,age,numFriends):
        total=0
        count=0
        for x in numFriends:
            total+=x
            count+=1
            yield age,total/count

if __name__=='__main__':
    MRFriendsByAge.run()