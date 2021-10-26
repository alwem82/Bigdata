import boto3, sys
import pandas as pd
import csv

ep_name = 'Octank-Sagemaker'
sm_rt = boto3.Session().client('runtime.sagemaker')

num = 0

with open('application_test.csv') as f:
    lines = f.readlines()
    row_count = sum(1 for row in lines)
    print(row_count)

    for l in lines[1:row_count]:   # Skip header
        num = num + 1
        l = l.split(',')      # Split CSV line into features
        sk_id_curr = l[0]
        #label = l[-1]         # Store 'yes'/'no' label
        #l = l[:-1]            # Remove label
        l = ','.join(l)       # Rebuild CSV line without label      
        response = sm_rt.invoke_endpoint(EndpointName=ep_name, 
                                         ContentType='text/csv',       
                                         Accept='text/csv', Body=l)

        response = response['Body'].read().decode("utf-8")
        #print ("SK_Id_CURR %s response %s" %(sk_id_curr,response))
        #print ("response %s" %(response))
        #wr.writerow(['sk_id_curr','response'])
        data = [[sk_id_curr,response[0:1]]]
        if num ==1 :
            submission = pd.DataFrame(data, columns=['SK_ID_CURR', 'TARGET'])
        else :
            #submission = submission.append(data,ignore_index=True)
            submission = submission.append({'SK_ID_CURR':sk_id_curr, 'TARGET':response[0:1]}, ignore_index=True)

        

submission.columns

submission.to_csv('submission.csv', index=False)
