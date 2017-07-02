
import numpy as np
import queue
import json
import sys

#
# this function finds the max id of users so that one can create nxn friend matrix
#
def get_max_id(JsonData):
	max_id = 0;
	for x in JsonData:
		if x["event_type"] == "purchase":
			if int(x["id"]) > max_id: 
				max_id = int(x["id"])
	return max_id

#
# update the purchase history of each user, if the queue is longer than the tracked number, remove the oldest purchase
#
def update_purchaseHistory(id, amount, userPurchaseHistoryList, TRACKED_NUMBER):
	userPurchaseHistoryList[id].put(amount)
	if userPurchaseHistoryList[id].qsize() > TRACKED_NUMBER:
		userPurchaseHistoryList[id].get()

#
# update the nxn friend matrix, 1 represents two people are friends 
#
def add_friend(id1, id2, friendMatrix):
	friendMatrix[id1][id2] = 1
	friendMatrix[id2][id1] = 1
	return friendMatrix
#
# update the nxn friend matrix, 1 represents two people are friends  
#
def remove_friend(id1,id2, friendMatrix):
	friendMatrix[id1][id2] = 0
	friendMatrix[id2][id1] = 0
	return friendMatrix


#
# get the average of user's purchase history
#
def get_mean(id, userPurchaseHistoryList):
	tmpList = list(userPurchaseHistoryList[id].queue)
	sum = 0.0
	for x in tmpList:
		sum+= float(x)
	return sum/len(tmpList)


#
# get the std dev of user's purchase history
#
def get_std_dev(id,userPurchaseHistoryList):
	tmpList = list(userPurchaseHistoryList[id].queue)
	mean = get_mean(id,userPurchaseHistoryList)
	tmpSum = 0
	for x in tmpList:
		tmpSum += (float(x)-mean)**2
	return np.sqrt( (1/len(tmpList))*tmpSum )


#
# check whether new pruchase is is higher than mean+3sigma
#
def is_anomaly(id, amount, userPurchaseHistoryList):
	mean = get_mean(id,userPurchaseHistoryList)
	std_dev = get_std_dev(id,userPurchaseHistoryList)
	
	if amount > mean + 3*std_dev:
		return True
	else:
		return False


#
# get the friend list according to degrees of freedom.
#
def get_friendList(id, friendMatrix, friendList, DEGREES_OF_FREEDOM):
	if DEGREES_OF_FREEDOM > 1:
		# in case of d.o.f is bigger than 1, do recursive function call
		idx = 1
		while idx<len(friendMatrix[id]):
			if friendMatrix[id][idx] == 1 and idx not in friendList:
				friendList.append(idx)
				friendList =  get_friendList(idx,friendMatrix,friendList,DEGREES_OF_FREEDOM-1)
			idx +=1
	else:
		idx = 1
		while idx<len(friendMatrix[id]):
			if friendMatrix[id][idx] == 1 and idx not in friendList:
				friendList.append(idx)
			idx +=1
	return friendList

######################################################
######################################################
# open the batch_log file and store it as batch_log  #
######################################################
######################################################
data_file_name = sys.argv[1]
stream_file_name = sys.argv[2]
output_file_name = sys.argv[3]
email_file_name = sys.argv[4]

#print(data_file_name,stream_file_name,output_file_name)

print("Opening the data file...")
data_file = open(data_file_name, "r")
batch_log = [json.loads(line) for line in data_file]

# get the tracked number and d.o.f
TRACKED_NUMBER = int(batch_log[0]["T"])
DEGREES_OF_FREEDOM = int(batch_log[0]["D"])

# delete the first line which includes TRACKED_NUMBER and DEGREES_OF_FREEDOM
del batch_log[0]


# get the max id and create a friend list with adjacency matrix method
max_ID = get_max_id(batch_log)
friendMatrix = np.zeros((max_ID+1,max_ID+1))





# create list of purchase history for each user, each element of the list is a queue
userPurchaseHistoryList = []
idx = 0
while idx<=max_ID:
	userPurchaseHistoryList.append(queue.Queue())
	idx = idx + 1

# loop over the lines of the log file, each line is a python dictionary
for dic in  batch_log:
	if dic["event_type"] == "purchase" :
		update_purchaseHistory(int(dic["id"]), float(dic["amount"]),userPurchaseHistoryList,TRACKED_NUMBER)

	elif dic["event_type"] == "befriend":
		friendMatrix = add_friend(int(dic["id1"]),int(dic["id2"]),friendMatrix)

	else :
		friendMatrix = remove_friend(int(dic["id1"]),int(dic["id2"]),friendMatrix)



######################################################
######################################################
# open the stream_log file and store it as stream_data
######################################################
######################################################
print("Opening the stream file...")
stream_file = open(stream_file_name, "r")
stream_log = [json.loads(line) for line in stream_file]


# open the flagged_purchases.json file to write anomalies
output_file = open(output_file_name, "w")


# open the email_tobe_send.txt file to write who to notify after an anomly purchase
output_file_for_friends = open(email_file_name, "w")

# check whether the friend matrix needs and update?, in our case it is not 
print("Max ID in bacth file is ", max_ID, " and max ID in stream file is ",get_max_id(stream_log))


friendListtobeEmailed = []

for dic in stream_log:
	if dic["event_type"] == "purchase" :
		# if the purchase is anomaly, raise a flag also create an email list
		if is_anomaly(int(dic["id"]), float(dic["amount"]),userPurchaseHistoryList) :
			mean = get_mean(int(dic["id"]), userPurchaseHistoryList)
			std_dev = get_std_dev(int(dic["id"]),userPurchaseHistoryList)
			print ("Writing the flagged purchase from user ", dic["id"], " with ", dic["amount"], " dollars." )
			mean_tobeWritten = "{0:.2f}".format(mean) # only 2 decimal points 
			std_dev_tobeWritten =  "{0:.2f}".format(std_dev) # only 2 decimal points 
			output_file.write("{\"event_type\":\"purchase\", \"timestamp\":\""+ dic["timestamp"]+"\""+", \"id\": \""+dic["id"]+"\""+", \"amount\":\""+ dic["amount"]+"\""+", \"mean\":\""+ mean_tobeWritten +"\""+", \"sd\":\""+ std_dev_tobeWritten +"\"}"+"\n")

			


			##### fill the friend list to be notified ##############
			
			# add the buyer to the list first, later we will make sre that all the users are unique in the list and remove the buyer 
			friendListtobeEmailed.append(int(dic["id"]))
			friendListtobeEmailed = get_friendList(int(dic["id"]),friendMatrix,friendListtobeEmailed,DEGREES_OF_FREEDOM)
			del friendListtobeEmailed[0]
			print("Writing  email list to a txt file")
			output_file_for_friends.write("\nBuyer: " + dic["id"] + " Time Stamp: " + dic["timestamp"] + " Amount: "+ dic["amount"] + " Email List: ")
			for x in friendListtobeEmailed:
				output_file_for_friends.write( str(x) + " ")

			output_file_for_friends.write("\n")

		# update the purchse history again
		update_purchaseHistory(int(dic["id"]), float(dic["amount"]),userPurchaseHistoryList,TRACKED_NUMBER)

	elif dic["event_type"] == "befriend":
		friendMatrix = add_friend(int(dic["id1"]),int(dic["id2"]),friendMatrix)

	else :
		friendMatrix =  remove_friend(int(dic["id1"]),int(dic["id2"]),friendMatrix)



data_file.close()
stream_file.close()
output_file.close()







