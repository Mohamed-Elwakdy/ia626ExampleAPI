import json,pymysql,time
from flask import Flask
from flask import request,redirect

app = Flask(__name__)
res = {}
AUTHKEY = '123'  
res1 ={}
res2 ={}

@app.route("/getMinMax", methods=['GET','POST']) 
def getMinMax():
    
    qstart = request.args.get('start')
    qend = request.args.get('end')
    #auth:
    qkey = request.args.get('key')
    if qkey is None or qkey != AUTHKEY:
        res['code'] = 0
        res['msg'] = 'Bad key given'
        res['req'] = 'getMinMax'
        return json.dumps(res,indent=4)
    elif qstart is None or qend is None:
        res['code'] = 2                       
        res['msg'] = 'Missing end or start key.'
        res['req'] = 'getMinMax'
        return json.dumps(res,indent=4) 
    
    conn = pymysql.connect(host='mysql.clarksonmsda.org', port=3306, user='ia626',passwd='ia626clarkson', db='ia626', autocommit=True) #setup our credentials
    cur = conn.cursor(pymysql.cursors.DictCursor)
    sql = 'SELECT *,MIN(`Depth`) AS mindepth, MAX(`Depth`) AS maxdepth FROM `snow` WHERE `Date` BETWEEN %s AND %s ORDER BY `Date`'
    print('range:',qstart,qend)
    start = time.time()
    cur.execute(sql,(qstart,qend)) 
    res['code'] = 1
    res['msg'] = 'Request  OK'
    res['req'] = 'getMinMax'
    res['sqltime'] = time.time() - start
    d= {}
    for row in cur:              
        d['min'] = row['mindepth'] 
        d['max'] = row['maxdepth']
    res['data'] = d
    return json.dumps(res,indent=4)

##################

@app.route("/getData", methods=['GET','POST']) 
def getData():
    
    
    qstart1 = request.args.get('start')
    qend1 = request.args.get('end')
    #auth:
    qkey1 = request.args.get('key')
    
    if qkey1 is None or qkey1 != AUTHKEY:
        res1['code'] = 0
        res1['msg'] = 'Bad key given'
        res1['sqltime'] = 0
        res1['req'] = 'getData'
        return json.dumps(res1,indent=4)
    elif qstart1 is None or qend1 is None:       
        res1['code'] = 2                        
        res1['msg'] = 'Missing end or start key.'
        res1['sqltime'] = 5
        res1['req'] = 'getData'
        return json.dumps(res1,indent=4) 

    conn = pymysql.connect(host='mysql.clarksonmsda.org', port=3306, user='ia626',passwd='ia626clarkson', db='ia626', autocommit=True) #setup our credentials
    cur = conn.cursor(pymysql.cursors.DictCursor)
    sql = 'SELECT * FROM `snow` WHERE `Date` BETWEEN %s AND %s ORDER BY `Date`'
    print('range:',qstart1,qend1)
    start = time.time()
    cur.execute(sql,(qstart1,qend1))
    res1['code'] = 1
    res1['msg'] = 'Request ok'
    res1['sqltime']= time.time() - start
    res1['req'] = 'getData'
    res1['data'] = []

    for row in cur:
        dict = {}
        dict['Date'] = str(row['Date']) 
        dict['Depth'] = row['Depth']
        res1['data'].append(dict)
    return json.dumps(res1,indent=4)

##################

@app.route("/getMean", methods=['GET','POST'])

def getMean():

    qstart2 = request.args.get('start')
    qend2 = request.args.get('end')
    #auth:
    qkey2 = request.args.get('key')

    if qkey2 is None or qkey2 != AUTHKEY:
        res2['code'] = 0
        res2['msg'] = 'Bad key given'
        res2['sqltime'] = 0
        res2['req'] = 'getMean'
        return json.dumps(res2,indent=4)
    
    elif qstart2 is None or qend2 is None:       
        res2['code'] = 2                        
        res2['msg'] = 'Missing end or start key.'
        res2['sqltime'] = 5
        res2['req'] = 'getMean'
        return json.dumps(res2,indent=4)

    conn = pymysql.connect(host='mysql.clarksonmsda.org', port=3306, user='ia626',passwd='ia626clarkson', db='ia626', autocommit=True) #setup our credentials
    cur = conn.cursor(pymysql.cursors.DictCursor)
    sql = 'SELECT *,AVG(`Depth`) AS avgdepth FROM `snow` WHERE `Date` BETWEEN %s AND %s ORDER BY `Date`'
    print('range:',qstart2,qend2)
    start = time.time()
    cur.execute(sql,(qstart2,qend2))
    res2['code'] = 1
    res2['msg'] = 'Request  OK'
    res2['req'] = 'getMean'
    res2['sqltime']= time.time() - start
    
    #127.0.0.1:5000/getMean?key=123&start=2015-05-17&end=2015-05-19
    
    dict = {}
    
    for row in cur:
        
        dict['mean'] = str (row['avgdepth'])
        res2['data'] = dict
    
    return json.dumps(res2,indent=4)


if __name__ == "__main__":
    app.run(host='127.0.0.1',debug=True)


