import json,pymysql,time

from flask import Flask
from flask import request,redirect



app = Flask(__name__)
res = {} 
AUTHKEY = '123'
@app.route("/", methods=['GET','POST'])
def root():
    res['code'] = 2
    res['msg'] = 'No endpoint specified'
    res['req'] = '/'
    return json.dumps(res,indent=4)

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
    d = {}
    for row in cur:
        d['min'] = row['mindepth']
        d['max'] = row['maxdepth']
    res['data'] = d
    return json.dumps(res,indent=4)
        
if __name__ == "__main__":
    app.run(host='127.0.0.1',debug=True)

    
    
    
    
    