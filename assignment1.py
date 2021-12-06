#!/usr/bin/env python
# coding: utf-8

# In[39]:


##import the libraries needed 
import pandas as pd 
import json 


# In[40]:


##read data from our csv file  to be able to analyze it 
##please provide the read_csv() with the correct path for the data set file to be able to read it 
data = pd.read_csv('graph_data.csv')
data 


# In[41]:


##FIRST QUESTION
##1. How many edges are there per webpage?

##for the edge calculation ,, I assume that the no. of edges = no. of src attributes = no. of dst attribute
##This assumption is based on the fact that the edge is the interaction between two resources
##for example , take the first row , it is a requst from resource X as a source to resource Y as a destination
##Assuming that X,Y are different resources , so counting X entries or Y entries only which is (no. of rows in srs or dst columns) will give me the edge


##groubby( ) takes our dataframe and groub it based on what we passed there as an argument
##thanks to people who develop libraries they make our life easier, we have count function which will count things per each group
counted_data=data.groupby('visit_id').count() 
only_need_columns=counted_data[["src"]]

## use rename() function to make our visualized data meaningful 
only_need_columns.rename(columns={'src':'no. of edges'})

##Remember , our group here is webpage so we are done this way with the first question .

##running this will give us the question answer in a visualized shape.


# In[42]:


##2- How many edges had a response status of 200 for the website with visit ID = 1002?
##SOLUTION : 95

##based on the last question which we make a definition of the edge 
##we can proceed with second question 



##we will use groupby which I introduced in the last cell 
#after groubby grouped our data to groups of 4 groups which are the webpage groups 
##we will use get_group() to get a specific group which is 1002 here.
df_group_1002=(data.groupby("visit_id").get_group(1002))

##count() will do its work and returns statistics about the data
counted_response_data=df_group_1002.groupby("response_status").count()
response_status_columns=counted_response_data[["visit_id"]]
response_status_columns.rename(columns={'visit_id':'no. of occurrences'})

##running this cell will return the no.of occuernces for all response status not only 200 for group 1002
##btw, the answer here is 95


# In[43]:


##FOR QUESTION THREE ..SOLUTION : 291416
###3. The attr column shows a dictionary (in JSON format) with two parameters -- ctype and clength. clength is the length of the HTTP response. In this dataset, what is the maximum value of clength?
###the actual implementation 

##this is a bit tricky one , as the column have json format which we need to play with data to retrive it in a way we can then process it

##get the attr column only
attr_column=data[["attr"]]
##get its length as we will want to iterate on all of it
attr_column_len=len(attr_column)
##convert our dataFrame to json (the whole dataframe will be converted regardlless that it contains json internally)
attr_json=attr_column.to_json(orient='values')
##declaration of variables needed 
max_length=0
i=0
maximum_length_element_place=0
##iterate no. of times = the attr column length
while (i<attr_column_len):
        ##convert our json to series string using loads() 
        ##will do it for every item in the column which is now a list
        string_item=json.loads(attr_json)[i][0]
        #check if it is a valid entry as the data set has empty entries and not valid ones in thes column
        ## use startswith() and pass '{' as every valid json starts with curly brace
        if( string_item.startswith('{')):
            ##convert our series string to dictionary
            dict_item=json.loads(string_item)
            ##check if the dictionary has "clength" as a key
            ##if not so it is not a valid one to search for clength 
            if ("clength" in dict_item) :
                ##get the clength value from this valid key 
                max_length_tmp=dict_item['clength']
                ##in the same iteration , check if it is the maximum clength here
                if(max_length_tmp > max_length):
                    ## if yes , make it our longest clength and get the place where we find it to be able to get the whole object in case we need that
                    max_length=max_length_tmp
                    maximum_length_element_place=i
                    
         
     
        i+=1

print("Number of iterations" , i)        
print("You can find the longest element at index " , maximum_length_element_place)
##get the longest attribute
longest_attr=json.loads(attr_json)[maximum_length_element_place][0]
print("This is the longest attr " , longest_attr)
##get the longest clength which is 291416
print ("The longest attr length is" , json.loads(longest_attr)['clength'])


# In[44]:


##4-how many edges have a Cookie header? Find the maximum length of a Cookie header's value.
##please , run it to be able to see the solution

##extract the reqattr column only
reqattr_column=data[["reqattr"]]
##get its length
reqattr_column_len=len(reqattr_column)
##declare variables for our loop
i=0
no_of_cookie_occuernces=0
longest_cookie_header_value=""
index_of_longest_cookie_header_value=0
##iterate no. of time equals the length 
while ( i< reqattr_column_len):
    #access item by item using iloc[]
    reqattr_item=reqattr_column.iloc[i][0]
    #check if it is a valid header file 
    if(reqattr_item.startswith("[")):
        #convert the last string to list 
        reqattr_list=json.loads(reqattr_item)
        #loop and search through this list of lists to check for cookie header for every element
        j=0
        while(j<len(reqattr_list)):
            ##get every list from the list of lists
            #check for every element of the list if the header name equals "Cookie"
            if(reqattr_list[j][0]=="Cookie"):
                ##if yes , increment our counter
                no_of_cookie_occuernces+=1
                ##here , we check for the longest cookie value
                if(len(longest_cookie_header_value)< len(reqattr_list[j][1])):
                    #if it is the longest , retrive it and its place to be able to retrieve this header if we want
                    longest_cookie_header_value= reqattr_list[j][1]
                    index_of_longest_cookie_header_value=i    
            j+=1    
    i+=1

print("Cookie Header appeared " , no_of_cookie_occuernces , "times\n" )
print("This is the longest cookie here" , longest_cookie_header_value , "\n")
print("This the requst that has the longest header value" , reqattr_column.iloc[index_of_longest_cookie_header_value][0])


# In[45]:


##5. Similar to question 4, the respattr value contains a "Set-Cookie" header, which indicates that a cookie was set. In this dataset, how many cookies are set per webpage? Per webpage, are there any cookies that are set multiple times (i.e., the same cookie name is in the value of Set-Cookie)? List these cookie names. Hint: Check the format of a Set-Cookie HTTP header.
##for this one ,, I think it is the most tricky here :D
## I would like to implement a standalone function for it 
##to be able to run it on any webpage if you want in the future

##this function takes a webpage as a parameter and do the work needed
def set_cookie_retrieval(group, webpage_id):
    ##get the respattr column only
    respattr_column=group[["respattr"]]
    ##get the length as we will iterate
    respattr_len=len(respattr_column)
    ##declare variables needed to the loop
    i=0
    no_of_set_cookie_occuernces=0
    ##here , the dictionary is a super tool to demonstrate the problem of how will we know if cookie has been set multiple times
    ##At the end of the day , you will have a dictionary contains every cookie name and no. of its occurrences per webpage
    ##declare our lovely dictionary which will make life easier and faster
    set_multiple_times_cookies_dict={}
    ##Iterate no. of times = respattr column length
    while ( i< respattr_len):
        #access item by item and access the first part of it which is the list of lists ( still in string format) we need to search in
        respattr_item=respattr_column.iloc[i][0]
        #check it is a valid header file 
        if(respattr_item.startswith("[")):
            #convert the last string to list 
            respattr_list=json.loads(respattr_item)
            #loop and search through this list to check for set-cookie header for every element
            j=0
            while(j<len(respattr_list)):
                #check for every element of the list if the header name equals "Set-Cookie"
                ##our set-cookie is at index 0 
                if(respattr_list[j][0]=="Set-Cookie"):
                    ##if yes, increment our counter
                    no_of_set_cookie_occuernces+=1
                    ##assuming that the cookie name is at index one in the list of set-cookie
                    ##the cookie name is the string until the first semicolon
                    ##I figured that by analyzing data and I hope I am right
                    ##put the second element in the set-cookie list in a new list to be able to split it
                    set_cookie_name= respattr_list[j][1].split(';')
                    ##Now we successfully get the cookie name
                    ##check if it appears before
                    if( set_cookie_name[0] in set_multiple_times_cookies_dict.keys()):
                        ##access it and increase its count
                        set_multiple_times_cookies_dict[set_cookie_name[0]]=set_multiple_times_cookies_dict[set_cookie_name[0]]+1
                    
                    ##it appears for first time so add the name as a key in the dict and its value is set to one as a count for this key occuernces
                    else:
                        set_multiple_times_cookies_dict[set_cookie_name[0]]=1
                     
                j+=1    
        
        
        i+=1

    

    ##print no.of set-cookie occurences
    print("The set-Cookie header appears here for web page no.",webpage_id, " is " , no_of_set_cookie_occuernces , "\n")
    ##print the whole dictionary which has the valuable info needed in a visual way
    print ("Every set-cookie header in this webpage and its number of occuernces \n" , set_multiple_times_cookies_dict , "\n")


# In[46]:


##by running this cell and call the last function for any webpage , you will have the info you need

groupid_1000=data.groupby("visit_id").get_group(1000)
groupid_1001=data.groupby("visit_id").get_group(1001)
groupid_1002=data.groupby("visit_id").get_group(1002)
groupid_1003=data.groupby("visit_id").get_group(1003)
groupid_1004=data.groupby("visit_id").get_group(1004)
##groupid_X=data.groupby("visit_id").get_group(X)


##call the set_cookie retrieval() 
#it takes two arguments , webpage dataframe and its id
set_cookie_retrieval(groupid_1000, 1000)
#set_cookie_retrieval(groupid_1001,1001)
#set_cookie_retrieval(groupid_1002)
#set_cookie_retrieval(groupid_1003)
#set_cookie_retrieval(groupid_1004)
#set_cookie_retrieval(groupid_X,X)


# In[ ]:




