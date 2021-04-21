IP-System Observations
-----------------------
1.  For Hang state calculation, we need to compare job submit time with request sending time instead of time after
    response.
2.  Live pipeline should store based on pipeline name + checkpoint directory + topic + partition
    Issue - if user changed the checkpoint directory of any configured pipeline.

##Other Issues
##------------
##1.  Correct the live monitoring object creation on main method.



Data Loss
--------------
1.  Live and Main pipeline should write their skip offsets details into persisted queue.
2.  Data loss publisher will get skip offset data and publish to their respected data loss prevented topics.



Persisted Queue Key
-------------------
Key     -       FromTopicName_Partition_ToTopicName
Value   -       Single, Range, List


##Phase-I Development
##-------------------
##1.  Live should publish the skip offset.
##2.  Main should publish the skip offset on queue:
##    a.  For Kafka treatment
##    b.  For Outlier treatment
##        i.  Find the maximum value previous value between current (topic + partition) value and kafka earliest value for
##            setting data loss start range and end range can be same as set value.
##    c.  For skip treatment

Phase-II Development
--------------------
1.  Config changes for Live and Main with respect to data loss topic support for every configured pipeline.
2.  Read data for every FromTopicName_Partition_ToTopicName value and send to their respective topic.
3.  Check for timeout support in Kafka publisher api.
    a.  If yes  -   For timeout records, Log their offset details.
    b.  If No   -   Apply Circuit breaker design pattern.

Release Steps
----------------
1.  Add statement like - For any new pipeline deployment/Major changes/Checkpoint changes. First stop health monitoring
    utilities Live and Main both. Once user done with changes, User need to start both the utilities.


Question
--------
One question
    if due to any reason data loss section is getting error
    Then We should continue the utility for main operation or break.
My solution proposal:
    Provide configuration for user want to run continue utility even if data loss is trowing error.