import json
import pymongo
import yaml
client = pymongo.MongoClient("localhost",27017)
with open("example_tim_data.json") as f:
    data = json.loads(f.read())

with open("mongodbschema.yaml","r") as yaml_file:
    schema = yaml.load(yaml_file, yaml.Loader)


db = client.test_database
#collection
c = db["teaminmatch"]
doc_ids = []
for i in range(len(data)):
    fail = False
    for j in data[i]:
        if not isinstance(data[i][j],type(schema["team_in_match"][j])):
            fail = True
            break
    if not fail:
        doc_ids.append(c.insert_one(data[i]).inserted_id)

#team class
class Team:
    def __init__(self,tn):
        self.team_num = tn
        self.average_balls_scored = 0
        self.least_balls_scored = 999
        self.most_balls_scored = 0
        self.number_of_matches_played = 0
        self.percent_climb_success = 0
    def calc_avg_balls_scored(self):
        total = 0
        #looping through documents
        for i in doc_ids:
            i2 = c.find_one({"_id":i})
            if i2["team_num"]==self.team_num:
                total += i2["num_balls"]
        self.average_balls_scored = total/self.number_of_matches_played
        return self.average_balls_scored
    def calc_least_balls_scored(self):
        #looping through documents
        for i in doc_ids:
            i2 = c.find_one({"_id":i})
            if i2["team_num"]==self.team_num:
                if i2["num_balls"]<self.least_balls_scored:
                    self.least_balls_scored=i2["num_balls"]
        return self.least_balls_scored
    def calc_most_balls_scored(self):
        #looping through documents
        for i in doc_ids:
            i2 = c.find_one({"_id":i})
            if i2["team_num"]==self.team_num:
                if i2["num_balls"]>self.least_balls_scored:
                    self.least_balls_scored=i2["num_balls"]
        return self.least_balls_scored
    def calc_num_matches_played(self):
        #looping through documents
        self.number_of_matches_played=0
        for i in doc_ids:
            i2 = c.find_one({"_id":i})
            if i2["team_num"]==self.team_num:
                self.number_of_matches_played+=1
        return self.number_of_matches_played
    def calc_percent_climb_success(self):
        #looping through documents
        numclimb = 0
        for i in doc_ids:
            i2 = c.find_one({"_id":i})
            if i2["team_num"]==self.team_num:
                if i2["climbed"]:
                    numclimb+=1
        self.percent_climb_success=float(numclimb/self.number_of_matches_played)
        return self.percent_climb_success

teamnums=[]
teamCollection = db["team-collection"]
for i in doc_ids:
    i2 = c.find_one({"_id":i})
    if i2["team_num"] not in teamnums:
        #making a dictionary for the team and adding it to the collection
        teamnums.append(i2["team_num"])
        team = Team(i2["team_num"])
        teamdict = {"team_num":team.team_num}
        teamdict["number_of_matches_played"]=team.calc_num_matches_played()
        teamdict["average_balls_scored"]=team.calc_avg_balls_scored()
        teamdict["least_balls_scored"]=team.calc_least_balls_scored()
        teamdict["most_balls_scored"]=team.calc_most_balls_scored()
        teamdict["number_of_matches_played"]=team.calc_num_matches_played()
        teamdict["percent_climb_success"]=team.calc_percent_climb_success()
        teamCollection.insert_one(teamdict)
    