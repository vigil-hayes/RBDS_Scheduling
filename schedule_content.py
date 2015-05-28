import csv
import sys
import threading
import pickle
import random
import itertools

# Seed for Random scheduling algorithm
random.seed(3848)

userclusteringcoefficient=pickle.load(open('userclusteringcoefficient.p', 'rb'))
userfollows = pickle.load( open( "userfollows.p", "rb" ) )
userorigin=pickle.load(open("userorigin.p", "rb"))

pool={}
unique_pool={}
hour=3600
tdv_user=254
last_user=None
window=hour

# NOTE: No algorithm provided
if not sys.argv[1]:
	print "!!! NO ALGORITHM INDICATED !!!"
	print "Please enter an algorithm:"
	print "\tFCFS -- first come, first serve [best effort]"
	print "\tR -- randomized"
	print "\tN -- naive"
	print "\tNA -- naive + aging"
	print "\tC -- clustering"
	print "\tMMF -- min-max fairness"
	print "Run 'python schedule_content.py help' for assistance"
	sys.exit(1)

# NOTE: HELP
if(sys.argv[1]=="help"):
	print "Schedule content"
	print "Format: python schedule_content.py [FCFS/N/NA/C/CG] input_media_action output_schedule [time_win_sec: default=3600] [num_users: default=254] [socialconnections pickle file: deault=socialconnections.p]"
	print "Algorithms:"
	print "\tFCFS -- first come, first serve [best effort]"
	print "\tR -- Randomized"
        print "\tN -- naive"
        print "\tNA -- naive + aging"
        print "\tC -- clustering"
	print "\tMMF -- min-max fairness"
        sys.exit(0)

# NOTE: No input provided
if not sys.argv[2]:
	print "!!! NO INPUT FILE INDICATED !!!"
	print "Run 'python schedule_content.py help' for assistance"
	sys.exit(1)

# Set algorithm
alg=sys.argv[1]

# Set input
infile=sys.argv[2]

# Set output
outfile=sys.argv[3]

# Set whether this an eval of reservation
# There will be a socialconnections pickle file set
if len(sys.argv) > 6:
	socialconnections=pickle.load(open(sys.argv[6], "rb"))
else:
	socialconnections=pickle.load(open("socialconnections.p", "rb"))
rate=1.1875 # kbps
available_KB_chunks=(window*rate)/8

satisfieduser={}

# Set number of users
if len(sys.argv) > 5:
	num_user=float(sys.argv[5])
else:
	num_user=tdv_user

# Removes content items demanded by users
def removeContent(creator_id, contentstring, userrequests):
	for p in socialconnections.keys():
                if p.split("\t")[0]==creator_id:
                        receiver=int(p.split("\t")[1])
                        if receiver not in userrequests.keys():
				continue
			if contentstring in userrequests[receiver]:
                        	userrequests[receiver].pop(userrequests[receiver].index(contentstring))
	return userrequests

# Assigns content items demanded by a user
def assignDemand(creator_id, contentstring, userrequests):
	for p in socialconnections.keys():
		if p.split("\t")[0]==creator_id:
			receiver=int(p.split("\t")[1])
			if receiver not in userrequests.keys():
				userrequests[receiver]=[]
			userrequests[receiver].append(contentstring)
	return userrequests

# Checks to see if content creator is followed by one of a group of users
def followedBy(c):
	userlst=[]
	for p in socialconnections.keys():
		if p.split("\t")[0]==creator_id:
			receiver=p.split("\t")[1]
			userlst.append(receiver)
			
	triadicvals=0		
	for u in userlst:
		if u in userclusteringcoefficient.keys():
			triadicvals+=float(userclusteringcoefficient[u])
	return triadicvals

# Adds content object to the pool of potential broadcasts
def addToPool(media_id, current_time, number_followers, groups, media_type, creator_id=None):
	pool[current_time]=[media_id, number_followers, groups, media_type, creator_id]

# Use a lock to protect the pool of content users
thread_lock=threading.Lock()

# Schedules content
def scheduleContent(alg, start, userrequests={}):
		print "New call to schedule"
		if alg=="MMF":
			avail=available_KB_chunks
			with open(outfile, 'ab') as orderrecord:
				global last_user

				# Get the last serviced user
				if last_user==None:
                                	last_user=sorted(userrequests.keys())[0]
				# Sort users in order based on their id
				sortedusers=sorted(userrequests.keys())
				cycle_pivot=last_user
				number_cycle=0

				# Set the cycle pivot 
				if cycle_pivot == sortedusers[-1]:
					# if the last user served was the last
					# ordered user, set the cycle piv
					cycle_pivot=sortedusers[0]

				# A list of users who no longer have data in their queues
				empty_users=[]
	
				# Check to see how many users have requests
				total_req_users=0
				for u in sortedusers:
					if len(userrequests[u]) > 0:
						total_req_users+=1

				# This loops through all users (requesting content and not)
				for u in itertools.cycle(sortedusers):
					number_cycle+=1
					# We have gone through an entire loop
					# and there is still space in the broadcast
					# queue, so set the cycle pivot to 0
					if number_cycle==len(sortedusers):
						cycle_pivot=0
					
					# Check to see if the user has any requests left
					if len(userrequests[u]) == 0 and u not in empty_users:
						empty_users.append(u)
						continue

					# If the user comes after the cycle pivot, 
					# add its next request to the list
					elif u >= cycle_pivot and len(userrequests[u]) > 0:
						last_user=u
						data=sorted(userrequests[u])[0].split("\t")
						orderrecord.write("%s\t%s\t%s\t%s\n" % (data[0], data[2], start-float(data[1]), data[3]))
						content_string=userrequests[u].pop(userrequests[u].index("\t".join(data)))
						userrequests=removeContent(data[4], content_string, userrequests)
						avail=avail-20
						if avail < 20:
							return
					# Check to see if all users are empty
					if len(empty_users) >= total_req_users:
						return
					
		if alg=="FCFS":
			avail=available_KB_chunks
			with open(outfile, 'ab') as orderrecord:
				for c in sorted(pool.keys()):
					data=("%s\t%s\t%s\t%s\t%s" % (pool[c][1], c, pool[c][0], pool[c][3], c)).split("\t")
                                	media_type=data[3]
                                	if media_type == "image":
                                        	uses=20
                                	elif media_type == "video":
                                        	uses=240
					if avail >= uses:
                                		pool.pop(float(data[1]))
                                        	avail=avail-uses
                                        	orderrecord.write("%s\t%s\t%s\t%s\n" % (data[0], data[2], start-float(data[1]), media_type))
                                	else:
                                		break
		if alg=="R":
			avail=available_KB_chunks
			order=[]
			uses=0
			for c in pool.keys():
                                order.append("%s\t%s\t%s\t%s\t%s" % (pool[c][1], c, pool[c][0], pool[c][3], c))
			with open(outfile, 'ab') as orderrecord:
				while 1:
					if len(order) == 0:
						break
					index=random.randint(0, len(order)-1)
					data=order.pop(index).split("\t")
					media_type=data[3]
                                	if media_type == "image":
                                        	uses=20
                                	elif media_type == "video":
                                        	uses=240
					if avail <  uses:
						break
					pool.pop(float(data[1]))
					avail=avail-uses
					orderrecord.write("%s\t%s\t%s\t%s\n" % (data[0], data[2], start-float(data[1]), media_type))

		if alg=="N" or alg=="C":
			avail=available_KB_chunks
			order=[]
			for c in pool.keys():
				order.append("%s\t%s\t%s\t%s\t%s" % (pool[c][1], 1.0/c, pool[c][0], pool[c][3], c))
		
			# For the content that was pooled
			# as long as there is available scheduling time
			# schedule the next most popular object
			for i in sorted(order, reverse=True):
				with open(outfile, 'ab') as orderrecord:
					data=i.split("\t")
					media_type=data[3]
					if media_type == "image":
						uses=20
					elif media_type == "video":
						uses=240
					else:
						uses=20
					if avail >= uses:
						pool.pop(float(data[4]))
						avail=avail-uses
						orderrecord.write("%s\t%s\t%s\t%s\n" % (data[0], data[2], start-float(data[4]), media_type))
					else:
						break
		# Naive+Aging
		if alg=="NA":
	                avail=available_KB_chunks
        	        order=[]
        		for c in pool.keys():
				if (start-c) > (2*window):
					order.append("%s\t%s\t%s\t%s\t%s" % (float(pool[c][1])+float(0.1*num_user), c, pool[c][0], pool[c][3], c))
				else:
        	                	order.append("%s\t%s\t%s\t%s\t%s" % (pool[c][1], c, pool[c][0], pool[c][3], c))

                	# For the content that was pooled
	                # as long as there is available scheduling time
        	        # schedule the next most popular object
                	for i in sorted(order, reverse=True):
                        	with open(outfile, 'ab') as orderrecord:
                                	data=i.split("\t")
                                	media_type=data[3]
	                                if media_type == "image":
        	                                uses=20
                		        if media_type == "video":
                                      		uses=240
                                	if avail >= uses:
                                        	pool.pop(float(data[1]))
	                                        avail=avail-uses
                	                        orderrecord.write("%s\t%s\t%s\n" % (data[0], data[2], start-float(data[1])))

# Open input file
with open(infile, 'rb') as csvfile:
	# NOTE: delimiter is tab
	csvreader=csv.reader(csvfile, delimiter="\t")
	count=0
	start_time=0
	end_time=0
	groups=[]
	userrequests={}
	for row in csvreader:
		if count==0:
			# Establish start time
			start_time=float(row[0])
			end_time=float(start_time+window)
			count+=1
		current_time=float(row[0])
		media_id_prefix=row[1]
		creator_id=row[2]
		media_id="%s_%s" % (media_id_prefix, creator_id)
		number_followers=float(row[13])
		cluster_coefficient=float(row[14])
		media_type=row[7]
		if current_time > end_time:
			scheduleContent(alg, current_time, userrequests)
			start_time=end_time
			end_time+=window
		if alg =="FCFS":
			if media_type == "image":
                        	addToPool(media_id, current_time, number_followers, groups, media_type)
		if alg =="R":
			if media_type == "image":
				addToPool(media_id, current_time, number_followers, groups, media_type)
		if alg =="N":
			if media_type == "image":
				addToPool(media_id, current_time, number_followers, groups, media_type)
		if alg == "C":
			if media_type == "image":
				addToPool(media_id, current_time, cluster_coefficient, groups, media_type, creator_id)
		if alg == "MMF":
			if media_type == "image":
				userrequests=assignDemand(creator_id, "%s\t%s\t%s\t%s\t%s" % (number_followers, 1.0/current_time, media_id, media_type, creator_id), userrequests)
